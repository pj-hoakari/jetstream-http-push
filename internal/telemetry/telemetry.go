// Package telemetry wires the OpenTelemetry tracing pipeline for the service.
package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace/noop"
)

// DefaultServiceName is reported as service.name when OTEL_SERVICE_NAME is unset.
const DefaultServiceName = "jetstream-http-push"

const (
	envServiceName     = "OTEL_SERVICE_NAME"
	envOTLPEndpoint    = "OTEL_EXPORTER_OTLP_ENDPOINT"
	envOTLPTracesPoint = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
)

// ShutdownFunc flushes buffered spans and releases the tracing pipeline.
type ShutdownFunc func(context.Context) error

// Setup installs the global tracer provider and the W3C trace context and
// baggage propagators.
//
// Spans are exported over OTLP/HTTP only when OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
// or OTEL_EXPORTER_OTLP_ENDPOINT is set. Without an endpoint the service keeps
// running with a no-op tracer provider instead of failing, so tracing stays an
// opt-in concern of the deployment environment. The remaining OTLP environment
// variables (headers, protocol-specific paths, TLS, timeouts) are honoured by
// the exporter itself.
//
// Whatever the pipeline reports about itself goes through the default slog
// logger, so that an export failure is a record like any other rather than a
// line on stderr no log backend reads.
//
// The returned ShutdownFunc must be called before the process exits so that
// buffered spans are flushed.
func Setup(ctx context.Context) (ShutdownFunc, error) {
	setupInternalLogging()

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	if !endpointConfigured() {
		otel.SetTracerProvider(noop.NewTracerProvider())

		return func(context.Context) error { return nil }, nil
	}

	res, err := newResource()
	if err != nil {
		return nil, err
	}

	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("create OTLP trace exporter: %w", err)
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(provider)

	return provider.Shutdown, nil
}

// setupInternalLogging routes what OpenTelemetry says about itself into slog.
// It runs whether or not an exporter is configured: it costs nothing, and a
// no-op pipeline simply never reports anything.
func setupInternalLogging() {
	otel.SetLogger(logr.FromSlogHandler(slog.Default().Handler()))
	// The error handler resolves the default logger per call, so a caller that
	// installs its logger after Setup is still heard.
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		slog.Error("opentelemetry error", "error", err)
	}))
}

// Enabled reports whether Setup exports spans with the current environment.
func Enabled() bool {
	return endpointConfigured()
}

// ServiceName returns the service.name reported by the tracing pipeline.
func ServiceName() string {
	if name := os.Getenv(envServiceName); name != "" {
		return name
	}

	return DefaultServiceName
}

func endpointConfigured() bool {
	for _, key := range []string{envOTLPTracesPoint, envOTLPEndpoint} {
		if os.Getenv(key) != "" {
			return true
		}
	}

	return false
}

func newResource() (*resource.Resource, error) {
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName(ServiceName())),
	)
	if err != nil {
		return nil, fmt.Errorf("build telemetry resource: %w", err)
	}

	return res, nil
}
