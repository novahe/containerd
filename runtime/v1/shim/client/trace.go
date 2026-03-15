/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package client

import (
	"context"

	"github.com/containerd/ttrpc"
	"go.opentelemetry.io/otel/propagation"
)

// traceInjectInjector injects OpenTelemetry trace context into ttrpc metadata
type traceInjectInjector struct {
	metadata ttrpc.MD
}

// Set implements propagation.TextMapCarrier
func (t *traceInjectInjector) Set(key, value string) {
	t.metadata.Set(key, value)
}

// Get implements propagation.TextMapCarrier
func (t *traceInjectInjector) Get(key string) string {
	if list, ok := t.metadata.Get(key); ok && len(list) > 0 {
		return list[0]
	}
	return ""
}

// Keys implements propagation.TextMapCarrier
func (t *traceInjectInjector) Keys() []string {
	keys := make([]string, 0, len(t.metadata))
	for k := range t.metadata {
		keys = append(keys, k)
	}
	return keys
}

// withTraceContext injects OpenTelemetry trace context into the ttrpc context
func withTraceContext(ctx context.Context) context.Context {
	// Get existing metadata from context (e.g., namespace)
	md, ok := ttrpc.GetMetadata(ctx)
	if !ok {
		md = ttrpc.MD{}
	} else {
		// Copy metadata to avoid modifying the original
		newMd := ttrpc.MD{}
		for k, v := range md {
			newMd.Set(k, v...)
		}
		md = newMd
	}

	// Inject trace context into metadata (no-op if tracing not configured)
	injector := &traceInjectInjector{metadata: md}
	propagator := propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
	propagator.Inject(ctx, injector)

	return ttrpc.WithMetadata(ctx, injector.metadata)
}

// newTraceInterceptor creates a new trace interceptor for ttrpc clients
func newTraceInterceptor() ttrpc.UnaryClientInterceptor {
	return func(ctx context.Context, req *ttrpc.Request, resp *ttrpc.Response, info *ttrpc.UnaryClientInfo, invoker ttrpc.Invoker) error {
		// Inject trace context before making the call
		ctx = withTraceContext(ctx)
		return invoker(ctx, req, resp)
	}
}
