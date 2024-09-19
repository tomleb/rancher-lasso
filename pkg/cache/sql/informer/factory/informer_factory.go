/*
Package factory provides a cache factory for the sql-based cache.
*/
package factory

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/rancher/lasso/pkg/cache/sql/db"
	"github.com/rancher/lasso/pkg/cache/sql/encryption"
	"github.com/rancher/lasso/pkg/cache/sql/informer"
	sqlStore "github.com/rancher/lasso/pkg/cache/sql/store"
	"github.com/rancher/lasso/pkg/log"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
)

// EncryptAllEnvVar is set to "true" if users want all types' data blobs to be encrypted in SQLite
// otherwise only variables in defaultEncryptedResourceTypes will have their blobs encrypted
const EncryptAllEnvVar = "CATTLE_ENCRYPT_CACHE_ALL"

// CacheFactory builds Informer instances and keeps a cache of instances it created
type CacheFactory struct {
	wg         wait.Group
	dbClient   DBClient
	stopCh     chan struct{}
	mutex      sync.RWMutex
	encryptAll bool

	newInformer newInformer

	informers      map[schema.GroupVersionKind]*guardedInformer
	informersMutex sync.RWMutex
}

type guardedInformer struct {
	informer *informer.Informer
	mutex    *sync.Mutex
}

type newInformer func(client dynamic.ResourceInterface, fields [][]string, transform cache.TransformFunc, gvk schema.GroupVersionKind, db sqlStore.DBClient, shouldEncrypt bool, namespace bool) (*informer.Informer, error)

type DBClient interface {
	informer.DBClient
	sqlStore.DBClient
	connector
}

type Cache struct {
	informer.ByOptionsLister
}

type connector interface {
	NewConnection() error
}

var defaultEncryptedResourceTypes = map[schema.GroupVersionKind]struct{}{
	{
		Version: "v1",
		Kind:    "Secret",
	}: {},
}

// NewCacheFactory returns an informer factory instance
func NewCacheFactory() (*CacheFactory, error) {
	m, err := encryption.NewManager()
	if err != nil {
		return nil, err
	}
	dbClient, err := db.NewClient(nil, m, m)
	if err != nil {
		return nil, err
	}
	return &CacheFactory{
		wg:          wait.Group{},
		stopCh:      make(chan struct{}),
		encryptAll:  os.Getenv(EncryptAllEnvVar) == "true",
		dbClient:    dbClient,
		newInformer: informer.NewInformer,
	}, nil
}

// CacheFor returns an informer for given GVK, using sql store indexed with fields, using the specified client. For virtual fields, they must be added by the transform function
// and specified by fields to be used for later fields.
func (f *CacheFactory) CacheFor(fields [][]string, transform cache.TransformFunc, client dynamic.ResourceInterface, gvk schema.GroupVersionKind, namespaced bool) (Cache, error) {
	// first of all block Reset() until we are done
	f.mutex.RLock()
	defer f.mutex.RUnlock()

	// then, check the cache. If we have a cached informer already, just return it
	f.informersMutex.RLock()
	// not deferring f.informersMutex.RUnlock() - needed to be able to upgrade RLock to Lock later
	gi, ok := f.informers[gvk]
	if ok && gi.informer != nil {
		f.informersMutex.RUnlock()
		return Cache{ByOptionsLister: gi.informer}, nil
	}

	// otherwise, if we have a mutex but not an informer yet, then some other
	// goroutine might be creating one. Wait for it to be done
	if ok && gi.informer == nil {
		gi.mutex.Lock()
		defer gi.mutex.Unlock()
	}

	// otherwise, it means no informer mutex was created yet. Create it and lock it
	if !ok {
		f.informersMutex.RUnlock()
		f.informersMutex.Lock()

		gi = &guardedInformer{
			informer: nil,
			mutex:    &sync.Mutex{},
		}
		gi.mutex.Lock()
		defer gi.mutex.Unlock()
		f.informers[gvk] = gi

		f.informersMutex.Unlock()
		f.informersMutex.RLock()
	}

	// past this point we are always holding an f.informersMutex.RLock and a gi.mutex.Lock

	// otherwise, create a new informer...
	start := time.Now()
	log.Infof("CacheFor STARTS creating informer for %v", gvk)
	defer func() {
		log.Infof("CacheFor ISDONE creating informer for %v (took %v)", gvk, time.Now().Sub(start))
	}()

	_, encryptResourceAlways := defaultEncryptedResourceTypes[gvk]
	shouldEncrypt := f.encryptAll || encryptResourceAlways
	i, err := f.newInformer(client, fields, transform, gvk, f.dbClient, shouldEncrypt, namespaced)
	if err != nil {
		f.informersMutex.RUnlock()
		return Cache{}, err
	}

	// ...store it in the informers cache...
	f.informersMutex.RUnlock()
	f.informersMutex.Lock()
	f.informers[gvk].informer = i
	f.informersMutex.Unlock()

	f.wg.StartWithChannel(f.stopCh, i.Run)
	if !cache.WaitForCacheSync(f.stopCh, i.HasSynced) {
		return Cache{}, fmt.Errorf("failed to sync SQLite Informer cache for GVK %v", gvk)
	}

	// ...and return it
	return Cache{ByOptionsLister: i}, nil
}

// Reset closes the stopCh which stops any running informers, assigns a new stopCh, resets the GVK-informer cache, and resets
// the database connection which wipes any current sqlite database at the default location.
func (f *CacheFactory) Reset() error {
	if f.dbClient == nil {
		// nothing to reset
		return nil
	}

	// first of all wait until all CacheFor() calls that create new informers are finished. Also block any new ones
	f.mutex.Lock()
	defer f.mutex.Unlock()

	// now that we are alone, stop all informers created until this point
	close(f.stopCh)
	f.stopCh = make(chan struct{})
	f.wg.Wait()

	// and get rid of all references to those informers and their mutexes
	f.informersMutex.Lock()
	defer f.informersMutex.Unlock()
	f.informers = make(map[schema.GroupVersionKind]*guardedInformer)

	// finally, reset the DB connection
	err := f.dbClient.NewConnection()
	if err != nil {
		return err
	}

	return nil
}
