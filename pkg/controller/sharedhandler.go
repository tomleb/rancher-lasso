package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rancher/lasso/pkg/metrics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	ErrIgnore = errors.New("ignore handler error")
)

type handlerEntry struct {
	id      int64
	name    string
	handler SharedControllerHandlerContext
}

type SharedHandler struct {
	// keep first because arm32 needs atomic.AddInt64 target to be mem aligned
	idCounter     int64
	controllerGVR string

	lock     sync.RWMutex
	handlers []handlerEntry
}

func (h *SharedHandler) Register(ctx context.Context, name string, handler SharedControllerHandler) {
	handlerContext := func(_ context.Context, key string, obj runtime.Object) (runtime.Object, error) {
		return handler.OnChange(key, obj)
	}
	h.RegisterContext(ctx, name, SharedControllerHandlerContextFunc(handlerContext))
}

func (h *SharedHandler) RegisterContext(ctx context.Context, name string, handler SharedControllerHandlerContext) {
	h.lock.Lock()
	defer h.lock.Unlock()

	id := atomic.AddInt64(&h.idCounter, 1)
	h.handlers = append(h.handlers, handlerEntry{
		id:      id,
		name:    name,
		handler: withTrace(name, handler),
	})

	go func() {
		<-ctx.Done()

		h.lock.Lock()
		defer h.lock.Unlock()

		for i := range h.handlers {
			if h.handlers[i].id == id {
				h.handlers = append(h.handlers[:i], h.handlers[i+1:]...)
				break
			}
		}
	}()
}

func (h *SharedHandler) OnChange(key string, obj runtime.Object) error {
	return h.OnChangeContext(context.Background(), key, obj)
}

func withTrace(name string, handler SharedControllerHandlerContext) SharedControllerHandlerContext {
	return SharedControllerHandlerContextFunc(func(parentCtx context.Context, key string, obj runtime.Object) (runtime.Object, error) {
		ctx, span := trace.SpanFromContext(parentCtx).TracerProvider().Tracer("").Start(parentCtx, fmt.Sprintf("running handler %s", name),
			trace.WithAttributes(attribute.KeyValue{Key: "lasso.controller.name", Value: attribute.StringValue(name)}),
		)
		defer span.End()

		newObj, err := handler.OnChangeContext(ctx, key, obj)
		if err != nil && !errors.Is(err, ErrIgnore) {
			span.RecordError(err)
			span.SetStatus(codes.Error, "something failed")
		}
		return newObj, err
	})
}

func (h *SharedHandler) OnChangeContext(ctx context.Context, key string, obj runtime.Object) error {
	var (
		errs errorList
	)
	h.lock.RLock()
	handlers := h.handlers
	h.lock.RUnlock()

	for _, handler := range handlers {
		var hasError bool
		reconcileStartTS := time.Now()

		newObj, err := handler.handler.OnChangeContext(ctx, key, obj)
		if err != nil && !errors.Is(err, ErrIgnore) {
			errs = append(errs, &handlerError{
				HandlerName: handler.name,
				Err:         err,
			})
			hasError = true
		}
		metrics.IncTotalHandlerExecutions(h.controllerGVR, handler.name, hasError)
		reconcileTime := time.Since(reconcileStartTS)
		metrics.ReportReconcileTime(h.controllerGVR, handler.name, hasError, reconcileTime.Seconds())

		if newObj != nil && !reflect.ValueOf(newObj).IsNil() {
			meta, err := meta.Accessor(newObj)
			if err == nil && meta.GetUID() != "" {
				// avoid using an empty object
				obj = newObj
			} else if err != nil {
				// assign if we can't determine metadata
				obj = newObj
			}
		}
	}

	return errs.ToErr()
}

type errorList []error

func (e errorList) Error() string {
	buf := strings.Builder{}
	for _, err := range e {
		if buf.Len() > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(err.Error())
	}
	return buf.String()
}

func (e errorList) ToErr() error {
	switch len(e) {
	case 0:
		return nil
	case 1:
		return e[0]
	default:
		return e
	}
}

func (e errorList) Cause() error {
	if len(e) > 0 {
		return e[0]
	}
	return nil
}

type handlerError struct {
	HandlerName string
	Err         error
}

func (h handlerError) Error() string {
	return fmt.Sprintf("handler %s: %v", h.HandlerName, h.Err)
}

func (h handlerError) Cause() error {
	return h.Err
}
