/*
Package controller contains interfaces, structs, and functions that can be used to create shared controllers for
managing clients, caches, and handlers for multiple types aka GroupVersionKinds.
*/
package controller

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rancher/lasso/pkg/log"
	"github.com/rancher/lasso/pkg/metrics"
	"github.com/rancher/lasso/pkg/workqueue"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	kworkqueue "k8s.io/client-go/util/workqueue"
)

const maxTimeout2min = 2 * time.Minute

type Handler interface {
	OnChange(key string, obj runtime.Object) error
}

type HandlerContext interface {
	OnChangeContext(ctx context.Context, key string, obj runtime.Object) error
}

type ResourceVersionGetter interface {
	GetResourceVersion() string
}

type HandlerFunc func(key string, obj runtime.Object) error

func (h HandlerFunc) OnChange(key string, obj runtime.Object) error {
	return h(key, obj)
}

type HandlerContextFunc func(ctx context.Context, key string, obj runtime.Object) error

func (h HandlerContextFunc) OnChangeContext(ctx context.Context, key string, obj runtime.Object) error {
	return h(ctx, key, obj)
}

type Controller interface {
	Enqueue(namespace, name string)
	EnqueueWithTrace(namespace, name string, trace *trace.SpanContext)
	EnqueueAfter(namespace, name string, delay time.Duration)
	EnqueueAfterWithTrace(namespace, name string, delay time.Duration, trace *trace.SpanContext)
	EnqueueKey(key string)
	EnqueueKeyWithTrace(key string, trace *trace.SpanContext)
	Informer() cache.SharedIndexInformer
	Start(ctx context.Context, workers int) error
}

type controller struct {
	startLock sync.Mutex

	name        string
	workqueue   workqueue.RateLimitingInterface
	rateLimiter kworkqueue.RateLimiter
	informer    cache.SharedIndexInformer
	handler     HandlerContext
	gvk         schema.GroupVersionKind
	startKeys   []startKey
	started     bool
	startCache  func(context.Context) error

	tracer trace.Tracer
}

type startKey struct {
	key   string
	trace any
	after time.Duration
}

type Options struct {
	RateLimiter            kworkqueue.RateLimiter
	SyncOnlyChangedObjects bool
	Tracer                 trace.Tracer
	GVK                    schema.GroupVersionKind
}

func New(name string, informer cache.SharedIndexInformer, startCache func(context.Context) error, handler Handler, opts *Options) Controller {
	handlerContext := func(_ context.Context, key string, obj runtime.Object) error {
		return handler.OnChange(key, obj)
	}
	return NewWithContext(name, informer, startCache, HandlerContextFunc(handlerContext), opts)
}

func NewWithContext(name string, informer cache.SharedIndexInformer, startCache func(context.Context) error, handler HandlerContext, opts *Options) Controller {
	opts = applyDefaultOptions(opts)

	tracer := opts.Tracer
	if tracer == nil {
		tracer = trace.NewNoopTracerProvider().Tracer("noop")
	}
	controller := &controller{
		name:        name,
		handler:     handler,
		informer:    informer,
		rateLimiter: opts.RateLimiter,
		startCache:  startCache,

		tracer: tracer,
		gvk:    opts.GVK,
	}

	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.handleObject,
		UpdateFunc: func(old, new interface{}) {
			if !opts.SyncOnlyChangedObjects || old.(ResourceVersionGetter).GetResourceVersion() != new.(ResourceVersionGetter).GetResourceVersion() {
				// If syncOnlyChangedObjects is disabled, objects will be handled regardless of whether an update actually took place.
				// Otherwise, objects will only be handled if they have changed
				controller.handleObject(new)
			}
		},
		DeleteFunc: controller.handleObject,
	})

	return controller
}

func applyDefaultOptions(opts *Options) *Options {
	var newOpts Options
	if opts != nil {
		newOpts = *opts
	}

	// from failure 0 to 12: exponential growth in delays (5 ms * 2 ^ failures)
	// from failure 13 to 30: 30s delay
	// from failure 31 on: 120s delay (2 minutes)
	if newOpts.RateLimiter == nil {
		newOpts.RateLimiter = kworkqueue.NewMaxOfRateLimiter(
			kworkqueue.NewItemFastSlowRateLimiter(time.Millisecond, maxTimeout2min, 30),
			kworkqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 30*time.Second),
		)
	}
	return &newOpts
}

func (c *controller) Informer() cache.SharedIndexInformer {
	return c.informer
}

func (c *controller) GroupVersionKind() schema.GroupVersionKind {
	return c.gvk
}

func (c *controller) run(workers int, stopCh <-chan struct{}) {
	c.startLock.Lock()
	// we have to defer queue creation until we have a stopCh available because a workqueue
	// will create a goroutine under the hood.  It we instantiate a workqueue we must have
	// a mechanism to Shutdown it down.  Without the stopCh we don't know when to shutdown
	// the queue and release the goroutine
	c.workqueue = workqueue.NewRateLimitingQueueWithConfig(c.rateLimiter, workqueue.RateLimitingQueueConfig{
		Name: c.name,
		DelayingQueue: workqueue.NewDelayingQueueWithConfig(workqueue.DelayingQueueConfig{
			Name: c.name,
			Queue: workqueue.NewWithConfig(workqueue.QueueConfig{
				Name: c.name,
				CoalesceFunc: func(acc, spanCtx any) any {
					if spanCtx == nil {
						return acc
					}

					span, ok := spanCtx.(*trace.SpanContext)
					if !ok || span == nil {
						return acc
					}

					var list []trace.SpanContext
					if acc != nil {
						list = acc.([]trace.SpanContext)
					}
					list = append(list, *span)
					return list
				},
			}),
		}),
	})
	for _, start := range c.startKeys {
		if start.after == 0 {
			c.workqueue.AddWithTrace(start.key, start.trace)
		} else {
			c.workqueue.AddAfterWithTrace(start.key, start.after, start.trace)
		}
	}
	c.startKeys = nil
	c.startLock.Unlock()

	defer utilruntime.HandleCrash()
	defer func() {
		c.workqueue.ShutDown()
	}()

	// Start the informer factories to begin populating the informer caches
	log.Infof("Starting %s controller", c.name)

	for i := 0; i < workers; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	<-stopCh
	c.startLock.Lock()
	defer c.startLock.Unlock()
	c.started = false
	log.Infof("Shutting down %s workers", c.name)
}

func (c *controller) Start(ctx context.Context, workers int) error {
	c.startLock.Lock()
	defer c.startLock.Unlock()

	if c.started {
		return nil
	}

	if err := c.startCache(ctx); err != nil {
		return err
	}

	if ok := cache.WaitForCacheSync(ctx.Done(), c.informer.HasSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	go c.run(workers, ctx.Done())
	c.started = true
	return nil
}

func (c *controller) runWorker() {
	for c.processNextWorkItem() {
	}
}

func (c *controller) processNextWorkItem() bool {
	obj, shutdown, spanCtxs := c.workqueue.GetWithTrace()

	gvk := c.GroupVersionKind()
	traceOpts := []trace.SpanStartOption{}
	traceOpts = append(traceOpts, trace.WithAttributes(
		attribute.KeyValue{
			Key:   "lasso.gvk.group",
			Value: attribute.StringValue(gvk.Group),
		},
		attribute.KeyValue{
			Key:   "lasso.gvk.version",
			Value: attribute.StringValue(gvk.Version),
		},
		attribute.KeyValue{
			Key:   "lasso.gvk.kind",
			Value: attribute.StringValue(gvk.Kind),
		},
	))
	if key, ok := obj.(string); ok {
		traceOpts = append(traceOpts, trace.WithAttributes(
			attribute.KeyValue{
				Key:   "lasso.key",
				Value: attribute.StringValue(key),
			},
		))
	}
	if spanCtxs != nil {
		for _, spanCtx := range spanCtxs.([]trace.SpanContext) {
			link := trace.Link{
				SpanContext: spanCtx,
			}
			traceOpts = append(traceOpts, trace.WithLinks(link))
		}
	}

	ctx, span := c.tracer.Start(context.Background(), "process next item", traceOpts...)
	defer span.End()

	if shutdown {
		return false
	}

	if err := c.processSingleItem(ctx, obj); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "some error")
		if !strings.Contains(err.Error(), "please apply your changes to the latest version and try again") {
			log.Errorf("%v", err)
		}
		return true
	}

	return true
}

func (c *controller) processSingleItem(ctx context.Context, obj interface{}) error {
	var (
		key string
		ok  bool
	)

	defer c.workqueue.Done(obj)

	if key, ok = obj.(string); !ok {
		c.workqueue.Forget(obj)
		log.Errorf("expected string in workqueue but got %#v", obj)
		return nil
	}
	if err := c.syncHandler(ctx, key); err != nil {
		spanCtx := trace.SpanContextFromContext(ctx)
		c.workqueue.AddRateLimitedWithTrace(key, &spanCtx)
		return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
	}

	c.workqueue.Forget(obj)
	return nil
}

func (c *controller) syncHandler(parentCtx context.Context, key string) error {
	obj, exists, err := c.informer.GetStore().GetByKey(key)
	if err != nil {
		metrics.IncTotalHandlerExecutions(c.name, "", true)
		return err
	}
	if !exists {
		return c.handler.OnChangeContext(parentCtx, key, nil)
	}

	acc, err := meta.Accessor(obj)
	if err != nil {
		return err
	}

	ctx, span := trace.SpanFromContext(parentCtx).TracerProvider().Tracer("").Start(parentCtx, "OnChange start",
		trace.WithAttributes(
			attribute.KeyValue{
				Key:   "lasso.metadata.name",
				Value: attribute.StringValue(acc.GetName()),
			},
			attribute.KeyValue{
				Key:   "lasso.metadata.namespace",
				Value: attribute.StringValue(acc.GetNamespace()),
			},
		),
	)
	defer span.End()

	return c.handler.OnChangeContext(ctx, key, obj.(runtime.Object))
}

func (c *controller) EnqueueKey(key string) {
	c.EnqueueKeyWithTrace(key, nil)
}

func (c *controller) EnqueueKeyWithTrace(key string, spanCtx *trace.SpanContext) {
	c.startLock.Lock()
	defer c.startLock.Unlock()

	if c.workqueue == nil {
		c.startKeys = append(c.startKeys, startKey{key: key, trace: spanCtx})
	} else {
		c.workqueue.AddWithTrace(key, spanCtx)
	}
}

func (c *controller) Enqueue(namespace, name string) {
	c.EnqueueWithTrace(namespace, name, nil)
}

func (c *controller) EnqueueWithTrace(namespace, name string, spanCtx *trace.SpanContext) {
	key := keyFunc(namespace, name)

	c.startLock.Lock()
	defer c.startLock.Unlock()

	if c.workqueue == nil {
		c.startKeys = append(c.startKeys, startKey{key: key, trace: spanCtx})
	} else {
		c.workqueue.AddRateLimitedWithTrace(key, spanCtx)
	}
}

func (c *controller) EnqueueAfter(namespace, name string, duration time.Duration) {
	c.EnqueueAfterWithTrace(namespace, name, duration, nil)
}

func (c *controller) EnqueueAfterWithTrace(namespace, name string, duration time.Duration, spanCtx *trace.SpanContext) {
	key := keyFunc(namespace, name)

	c.startLock.Lock()
	defer c.startLock.Unlock()

	if c.workqueue == nil {
		c.startKeys = append(c.startKeys, startKey{key: key, after: duration, trace: spanCtx})
	} else {
		c.workqueue.AddAfterWithTrace(key, duration, spanCtx)
	}
}

func keyFunc(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "/" + name
}

func (c *controller) enqueue(obj interface{}, spanCtx *trace.SpanContext) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		log.Errorf("%v", err)
		return
	}
	c.startLock.Lock()
	if c.workqueue == nil {
		c.startKeys = append(c.startKeys, startKey{key: key, trace: spanCtx})
	} else {
		c.workqueue.AddWithTrace(key, spanCtx)
	}
	c.startLock.Unlock()
}

func (c *controller) handleObject(obj interface{}) {
	if _, ok := obj.(metav1.Object); !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			log.Errorf("error decoding object, invalid type")
			return
		}
		newObj, ok := tombstone.Obj.(metav1.Object)
		if !ok {
			log.Errorf("error decoding object tombstone, invalid type")
			return
		}
		obj = newObj
	}
	_, span := c.tracer.Start(context.Background(), "handleObject")
	defer span.End()

	spanCtx := span.SpanContext()
	c.enqueue(obj, &spanCtx)
}
