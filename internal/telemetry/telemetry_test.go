package telemetry_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/pj-hoakari/jetstream-http-push/internal/logging"
	"github.com/pj-hoakari/jetstream-http-push/internal/telemetry"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestSetupWithoutEndpointKeepsTracingNoop(t *testing.T) {
	clearOTLPEndpoints(t)

	shutdown, err := telemetry.Setup(context.Background())
	if err != nil {
		t.Fatalf("Setup() error = %v", err)
	}

	if telemetry.Enabled() {
		t.Error("Enabled() = true, want false without an OTLP endpoint")
	}

	if _, ok := otel.GetTracerProvider().(noop.TracerProvider); !ok {
		t.Errorf("tracer provider = %T, want noop.TracerProvider", otel.GetTracerProvider())
	}

	if fields := otel.GetTextMapPropagator().Fields(); !slices.Contains(fields, "traceparent") {
		t.Errorf("propagator fields = %v, want to contain %q", fields, "traceparent")
	}

	if err := shutdown(context.Background()); err != nil {
		t.Errorf("shutdown() error = %v", err)
	}
}

func TestSetupWithEndpointInstallsSDKProvider(t *testing.T) {
	clearOTLPEndpoints(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318")

	shutdown, err := telemetry.Setup(context.Background())
	if err != nil {
		t.Fatalf("Setup() error = %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// No span is recorded in this test, so shutting down must not try to
		// reach the (absent) collector.
		if err := shutdown(ctx); err != nil {
			t.Errorf("shutdown() error = %v", err)
		}

		otel.SetTracerProvider(noop.NewTracerProvider())
	})

	if !telemetry.Enabled() {
		t.Error("Enabled() = false, want true with an OTLP endpoint")
	}

	if _, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); !ok {
		t.Errorf("tracer provider = %T, want *sdktrace.TracerProvider", otel.GetTracerProvider())
	}
}

// TestSetupReportsInternalErrorsThroughSlog checks that what the tracing
// pipeline says about itself becomes a log record rather than a line on stderr.
// It installs the process-wide default logger, so it must not run in parallel.
func TestSetupReportsInternalErrorsThroughSlog(t *testing.T) {
	clearOTLPEndpoints(t)

	var logs bytes.Buffer

	previous := slog.Default()

	slog.SetDefault(logging.NewLogger(&logs, logging.Options{Level: slog.LevelDebug, AddSource: false, ProjectID: ""}))
	t.Cleanup(func() { slog.SetDefault(previous) })

	shutdown, err := telemetry.Setup(context.Background())
	if err != nil {
		t.Fatalf("Setup() error = %v", err)
	}

	t.Cleanup(func() {
		if err := shutdown(context.Background()); err != nil {
			t.Errorf("shutdown() error = %v", err)
		}
	})

	otel.Handle(errors.New("boom"))

	var entry map[string]any

	if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &entry); err != nil {
		t.Fatalf("unmarshal %q: %v", logs.String(), err)
	}

	if got, want := entry["severity"], "ERROR"; got != want {
		t.Errorf("severity = %v, want %q", got, want)
	}

	if got, want := entry["message"], "opentelemetry error"; got != want {
		t.Errorf("message = %v, want %q", got, want)
	}

	if got, want := entry["error"], "boom"; got != want {
		t.Errorf("error = %v, want %q", got, want)
	}
}

func TestServiceName(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "")

	if got := telemetry.ServiceName(); got != telemetry.DefaultServiceName {
		t.Errorf("ServiceName() = %q, want %q", got, telemetry.DefaultServiceName)
	}

	t.Setenv("OTEL_SERVICE_NAME", "custom-service")

	if got := telemetry.ServiceName(); got != "custom-service" {
		t.Errorf("ServiceName() = %q, want %q", got, "custom-service")
	}
}

func clearOTLPEndpoints(t *testing.T) {
	t.Helper()

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
}
