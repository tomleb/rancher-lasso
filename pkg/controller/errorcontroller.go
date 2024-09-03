package controller

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/trace"
	"k8s.io/client-go/tools/cache"
)

type errorController struct {
	informer cache.SharedIndexInformer
}

func newErrorController() *errorController {
	return &errorController{
		informer: cache.NewSharedIndexInformer(nil, nil, 0, cache.Indexers{}),
	}
}

func (n *errorController) Enqueue(namespace, name string) {
}

func (n *errorController) EnqueueWithTrace(namespace, name string, spanCtx *trace.SpanContext) {
}

func (n *errorController) EnqueueAfter(namespace, name string, delay time.Duration) {
}

func (n *errorController) EnqueueAfterWithTrace(namespace, name string, delay time.Duration, spanCtx *trace.SpanContext) {
}

func (n *errorController) EnqueueKey(key string) {
}

func (n *errorController) EnqueueKeyWithTrace(key string, spanCtx *trace.SpanContext) {
}

func (n *errorController) Informer() cache.SharedIndexInformer {
	return n.informer
}

func (n *errorController) Start(ctx context.Context, workers int) error {
	return nil
}
