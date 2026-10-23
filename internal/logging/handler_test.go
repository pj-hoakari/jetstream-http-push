package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"
)

// The W3C trace context example identifiers, used wherever a test needs a trace
// it can recognise in the output.
const (
	testTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	testSpanID  = "00f067aa0ba902b7"
)

func TestSeverityUsesCloudLoggingNames(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		level slog.Level
		want  string
	}{
		"debug":          {level: slog.LevelDebug, want: "DEBUG"},
		"below info":     {level: slog.LevelInfo - 1, want: "DEBUG"},
		"info":           {level: slog.LevelInfo, want: "INFO"},
		"above info":     {level: slog.LevelInfo + 1, want: "INFO"},
		"warn":           {level: slog.LevelWarn, want: "WARNING"},
		"error":          {level: slog.LevelError, want: "ERROR"},
		"below critical": {level: LevelCritical - 1, want: "ERROR"},
		"critical":       {level: LevelCritical, want: "CRITICAL"},
		"above critical": {level: LevelCritical + 1, want: "CRITICAL"},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			logger := NewLogger(&buf, Options{Level: slog.LevelDebug - 4, AddSource: false, ProjectID: ""})
			logger.Log(context.Background(), tt.level, "hello")

			if got := decodeLine(t, &buf)[keySeverity]; got != tt.want {
				t.Errorf("%s = %v, want %q", keySeverity, got, tt.want)
			}
		})
	}
}

func TestRecordUsesCloudLoggingKeys(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	logger := NewLogger(&buf, Options{Level: slog.LevelInfo, AddSource: false, ProjectID: ""})
	logger.Info("server listening", "addr", ":8080")

	entry := decodeLine(t, &buf)

	if got, want := entry[keyMessage], "server listening"; got != want {
		t.Errorf("%s = %v, want %q", keyMessage, got, want)
	}

	if got, want := entry["addr"], ":8080"; got != want {
		t.Errorf("addr = %v, want %q", got, want)
	}

	// slog's own message and level keys must be gone; its time key happens to
	// be the one Cloud Logging reads, so it stays.
	for _, key := range []string{slog.MessageKey, slog.LevelKey} {
		if _, ok := entry[key]; ok {
			t.Errorf("entry has key %q, want it renamed", key)
		}
	}

	timestamp, ok := entry[keyTime].(string)
	if !ok {
		t.Fatalf("%s = %#v, want a string", keyTime, entry[keyTime])
	}

	if _, err := time.Parse(time.RFC3339Nano, timestamp); err != nil {
		t.Errorf("%s = %q, want RFC3339Nano: %v", keyTime, timestamp, err)
	}

	if !strings.HasSuffix(timestamp, "Z") {
		t.Errorf("%s = %q, want it in UTC", keyTime, timestamp)
	}
}

func TestSourceLocationIsAGroupWithAStringLine(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	logger := NewLogger(&buf, Options{Level: slog.LevelInfo, AddSource: true, ProjectID: ""})
	logger.Info("with source")

	entry := decodeLine(t, &buf)

	source, ok := entry[keySourceLocation].(map[string]any)
	if !ok {
		t.Fatalf("%s = %#v, want a group", keySourceLocation, entry[keySourceLocation])
	}

	if file, ok := source["file"].(string); !ok || !strings.HasSuffix(file, "handler_test.go") {
		t.Errorf("file = %#v, want this test file", source["file"])
	}

	// Cloud Logging types the line number as a string, so a number here would
	// make it drop the whole source location.
	if line, ok := source["line"].(string); !ok || line == "" || line == "0" {
		t.Errorf("line = %#v, want a non-zero line number as a string", source["line"])
	}

	if function, ok := source["function"].(string); !ok || !strings.Contains(function, "TestSourceLocationIsAGroupWithAStringLine") {
		t.Errorf("function = %#v, want this test function", source["function"])
	}
}

func TestTraceFieldsFollowTheContextSpan(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		projectID string
		wantTrace string
	}{
		"with a project ID":    {projectID: "example-project", wantTrace: "projects/example-project/traces/" + testTraceID},
		"without a project ID": {projectID: "", wantTrace: testTraceID},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			logger := NewLogger(&buf, Options{Level: slog.LevelInfo, AddSource: false, ProjectID: tt.projectID})
			logger.ErrorContext(contextWithSampledSpan(t), "internal error")

			assertTraceFields(t, decodeLine(t, &buf), tt.wantTrace)
		})
	}
}

func TestTraceFieldsAreAbsentWithoutASpan(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	logger := NewLogger(&buf, Options{Level: slog.LevelInfo, AddSource: false, ProjectID: "example-project"})
	logger.ErrorContext(context.Background(), "internal error")

	entry := decodeLine(t, &buf)

	for _, key := range []string{keyTrace, keySpanID, keyTraceSampled} {
		if _, ok := entry[key]; ok {
			t.Errorf("entry has key %q, want no trace fields without a span", key)
		}
	}
}

func TestDerivedHandlersKeepTraceFields(t *testing.T) {
	t.Parallel()

	t.Run("WithAttrs", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer

		logger := NewLogger(&buf, Options{Level: slog.LevelInfo, AddSource: false, ProjectID: "example-project"}).With("service", "jetstream-http-push")
		logger.ErrorContext(contextWithSampledSpan(t), "internal error")

		entry := decodeLine(t, &buf)

		if got, want := entry["service"], "jetstream-http-push"; got != want {
			t.Errorf("service = %v, want %q", got, want)
		}

		assertTraceFields(t, entry, "projects/example-project/traces/"+testTraceID)
	})

	t.Run("WithGroup", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer

		logger := NewLogger(&buf, Options{Level: slog.LevelInfo, AddSource: false, ProjectID: "example-project"}).WithGroup("request")
		logger.ErrorContext(contextWithSampledSpan(t), "internal error", "method", "Greet")

		entry := decodeLine(t, &buf)

		// Cloud Logging reads the trace fields at the top level only, so an
		// open group must not swallow them.
		assertTraceFields(t, entry, "projects/example-project/traces/"+testTraceID)

		group, ok := entry["request"].(map[string]any)
		if !ok {
			t.Fatalf("request = %#v, want a group", entry["request"])
		}

		// The record's own attributes still belong to the open group.
		if got, want := group["method"], "Greet"; got != want {
			t.Errorf("request.method = %v, want %q", got, want)
		}

		for _, key := range []string{keyTrace, keySpanID, keyTraceSampled} {
			if _, ok := group[key]; ok {
				t.Errorf("group has key %q, want the trace fields outside the group", key)
			}
		}
	})

	t.Run("WithAttrs after WithGroup", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer

		logger := NewLogger(&buf, Options{Level: slog.LevelInfo, AddSource: false, ProjectID: "example-project"}).WithGroup("request").With("method", "Greet")
		logger.ErrorContext(contextWithSampledSpan(t), "internal error")

		entry := decodeLine(t, &buf)

		assertTraceFields(t, entry, "projects/example-project/traces/"+testTraceID)

		group, ok := entry["request"].(map[string]any)
		if !ok {
			t.Fatalf("request = %#v, want a group", entry["request"])
		}

		if got, want := group["method"], "Greet"; got != want {
			t.Errorf("request.method = %v, want %q", got, want)
		}
	})
}

func TestParseLevel(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		in      string
		want    slog.Level
		wantErr bool
	}{
		"debug":         {in: "debug", want: slog.LevelDebug, wantErr: false},
		"info":          {in: "info", want: slog.LevelInfo, wantErr: false},
		"warn":          {in: "warn", want: slog.LevelWarn, wantErr: false},
		"warning":       {in: "warning", want: slog.LevelWarn, wantErr: false},
		"error":         {in: "error", want: slog.LevelError, wantErr: false},
		"critical":      {in: "critical", want: LevelCritical, wantErr: false},
		"upper case":    {in: "WARNING", want: slog.LevelWarn, wantErr: false},
		"mixed case":    {in: "Critical", want: LevelCritical, wantErr: false},
		"unknown":       {in: "verbose", want: 0, wantErr: true},
		"empty":         {in: "", want: 0, wantErr: true},
		"numeric":       {in: "4", want: 0, wantErr: true},
		"leading space": {in: " info", want: 0, wantErr: true},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseLevel(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseLevel(%q) error = %v, wantErr %t", tt.in, err, tt.wantErr)
			}

			if got != tt.want {
				t.Errorf("ParseLevel(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestReservedTopLevelAttrsAreRenamed(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	logger := NewLogger(&buf, Options{Level: slog.LevelInfo, AddSource: false, ProjectID: "example-project"})
	logger.ErrorContext(contextWithSampledSpan(t), "internal error",
		// A string under the time key must be written, not turned into a time.
		"time", "2020-01-01",
		"msg", "shadow message",
		"severity", "DEBUG",
		"level", "shadow level",
		"source", "shadow source",
		keyTrace, "shadow trace",
	)

	entry := decodeLine(t, &buf)

	if got, want := entry[keyMessage], "internal error"; got != want {
		t.Errorf("%s = %v, want %q", keyMessage, got, want)
	}

	if got, want := entry[keySeverity], "ERROR"; got != want {
		t.Errorf("%s = %v, want %q", keySeverity, got, want)
	}

	assertTraceFields(t, entry, "projects/example-project/traces/"+testTraceID)

	timestamp, ok := entry[keyTime].(string)
	if !ok {
		t.Fatalf("%s = %#v, want a string", keyTime, entry[keyTime])
	}

	if _, err := time.Parse(time.RFC3339Nano, timestamp); err != nil {
		t.Errorf("%s = %q, want the record's own timestamp: %v", keyTime, timestamp, err)
	}

	renamed := map[string]string{
		reservedPrefix + "time":     "2020-01-01",
		reservedPrefix + "msg":      "shadow message",
		reservedPrefix + "severity": "DEBUG",
		reservedPrefix + "level":    "shadow level",
		reservedPrefix + "source":   "shadow source",
		reservedPrefix + keyTrace:   "shadow trace",
	}

	for key, want := range renamed {
		if got := entry[key]; got != want {
			t.Errorf("%s = %v, want %q", key, got, want)
		}
	}
}

func TestReservedHandlerAttrsAreRenamed(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	logger := NewLogger(&buf, Options{Level: slog.LevelInfo, AddSource: false, ProjectID: ""}).With("msg", "shadow message")
	logger.Info("server listening")

	entry := decodeLine(t, &buf)

	if got, want := entry[keyMessage], "server listening"; got != want {
		t.Errorf("%s = %v, want %q", keyMessage, got, want)
	}

	if got, want := entry[reservedPrefix+"msg"], "shadow message"; got != want {
		t.Errorf("%s = %v, want %q", reservedPrefix+"msg", got, want)
	}
}

func TestReservedAttrsInsideAGroupKeepTheirNames(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	logger := NewLogger(&buf, Options{Level: slog.LevelInfo, AddSource: false, ProjectID: "example-project"}).WithGroup("payload")
	logger.ErrorContext(contextWithSampledSpan(t), "internal error", "msg", "inner", "time", "2020-01-01", keyTrace, "inner trace")

	entry := decodeLine(t, &buf)

	if got, want := entry[keyMessage], "internal error"; got != want {
		t.Errorf("%s = %v, want %q", keyMessage, got, want)
	}

	assertTraceFields(t, entry, "projects/example-project/traces/"+testTraceID)

	group, ok := entry["payload"].(map[string]any)
	if !ok {
		t.Fatalf("payload = %#v, want a group", entry["payload"])
	}

	// A name inside a group collides with nothing, so it is left alone.
	inGroup := map[string]string{"msg": "inner", "time": "2020-01-01", keyTrace: "inner trace"}

	for key, want := range inGroup {
		if got := group[key]; got != want {
			t.Errorf("payload.%s = %v, want %q", key, got, want)
		}
	}

	for key := range group {
		if strings.HasPrefix(key, reservedPrefix) {
			t.Errorf("payload has key %q, want nothing renamed inside a group", key)
		}
	}
}

func TestReplaceAttrLeavesANonTimeTimeAttrAlone(t *testing.T) {
	t.Parallel()

	tests := map[string]slog.Attr{
		"string":   slog.String(slog.TimeKey, "2020-01-01"),
		"duration": slog.Duration(slog.TimeKey, time.Second),
	}

	for name, attr := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := replaceAttr(nil, attr)
			if got.Key != attr.Key || got.Value.String() != attr.Value.String() {
				t.Errorf("replaceAttr(%v) = %v, want it unchanged", attr, got)
			}
		})
	}
}

func TestEnabledFollowsTheConfiguredLevel(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	handler := NewHandler(&buf, Options{Level: slog.LevelWarn, AddSource: false, ProjectID: ""})
	derived := handler.WithAttrs([]slog.Attr{slog.String("service", "jetstream-http-push")}).WithGroup("request")

	tests := map[string]struct {
		handler slog.Handler
		level   slog.Level
		want    bool
	}{
		"base below the threshold":    {handler: handler, level: slog.LevelInfo, want: false},
		"base at the threshold":       {handler: handler, level: slog.LevelWarn, want: true},
		"base above the threshold":    {handler: handler, level: LevelCritical, want: true},
		"derived below the threshold": {handler: derived, level: slog.LevelDebug, want: false},
		"derived above the threshold": {handler: derived, level: slog.LevelError, want: true},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if got := tt.handler.Enabled(context.Background(), tt.level); got != tt.want {
				t.Errorf("Enabled(%v) = %t, want %t", tt.level, got, tt.want)
			}
		})
	}
}

func TestDerivingWithNothingReturnsTheReceiver(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	handler := NewHandler(&buf, Options{Level: slog.LevelInfo, AddSource: false, ProjectID: ""})

	tests := map[string]slog.Handler{
		"WithGroup on an empty name": handler.WithGroup(""),
		"WithAttrs on nil":           handler.WithAttrs(nil),
		"WithAttrs on no attributes": handler.WithAttrs([]slog.Attr{}),
	}

	for name, derived := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if derived != handler {
				t.Errorf("%s returned a new handler, want the receiver", name)
			}
		})
	}
}

func TestSiblingHandlersKeepTheirOwnAttrs(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	shared := NewLogger(&buf, Options{Level: slog.LevelInfo, AddSource: false, ProjectID: ""}).With("service", "jetstream-http-push")
	shared.With("rpc", "Greet").Info("first")
	shared.With("tenant", "acme").Info("second")

	entries := decodeLines(t, &buf)
	if len(entries) != 2 {
		t.Fatalf("wrote %d records, want 2", len(entries))
	}

	first, second := entries[0], entries[1]

	for _, entry := range entries {
		if got, want := entry["service"], "jetstream-http-push"; got != want {
			t.Errorf("service = %v, want %q", got, want)
		}
	}

	if got, want := first["rpc"], "Greet"; got != want {
		t.Errorf("rpc = %v, want %q", got, want)
	}

	if got, want := second["tenant"], "acme"; got != want {
		t.Errorf("tenant = %v, want %q", got, want)
	}

	// Deriving twice from one handler must not let either sibling see the
	// other's attributes.
	if _, ok := first["tenant"]; ok {
		t.Errorf("first record = %v, want it without the sibling's attribute", first)
	}

	if _, ok := second["rpc"]; ok {
		t.Errorf("second record = %v, want it without the sibling's attribute", second)
	}
}

func TestHandleIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()

	const goroutines = 64

	writer := &syncWriter{}
	logger := NewLogger(writer, Options{Level: slog.LevelInfo, AddSource: false, ProjectID: "example-project"}).With("service", "jetstream-http-push").WithGroup("request")
	ctx := contextWithSampledSpan(t)

	var wg sync.WaitGroup

	wg.Add(goroutines)

	for i := range goroutines {
		go func() {
			defer wg.Done()

			logger.ErrorContext(ctx, "internal error", "worker", i)
		}()
	}

	wg.Wait()

	entries := decodeLines(t, &writer.buf)
	if len(entries) != goroutines {
		t.Fatalf("wrote %d records, want %d", len(entries), goroutines)
	}

	for _, entry := range entries {
		assertTraceFields(t, entry, "projects/example-project/traces/"+testTraceID)

		if got, want := entry["service"], "jetstream-http-push"; got != want {
			t.Errorf("service = %v, want %q", got, want)
		}

		if _, ok := entry["request"].(map[string]any); !ok {
			t.Errorf("request = %#v, want the record's attributes in the group", entry["request"])
		}
	}
}

// syncWriter serialises the writes of the concurrency test, so that the buffer
// itself is never the thing under contention.
type syncWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.buf.Write(p)
}

// assertTraceFields checks the three fields Cloud Logging correlates on.
func assertTraceFields(t *testing.T, entry map[string]any, wantTrace string) {
	t.Helper()

	if got := entry[keyTrace]; got != wantTrace {
		t.Errorf("%s = %v, want %q", keyTrace, got, wantTrace)
	}

	if got := entry[keySpanID]; got != testSpanID {
		t.Errorf("%s = %v, want %q", keySpanID, got, testSpanID)
	}

	if got := entry[keyTraceSampled]; got != true {
		t.Errorf("%s = %v, want true", keyTraceSampled, got)
	}
}

// contextWithSampledSpan returns a context carrying a valid, sampled span
// context, as a request that the tracing interceptor has already handled does.
func contextWithSampledSpan(t *testing.T) context.Context {
	t.Helper()

	traceID, err := trace.TraceIDFromHex(testTraceID)
	if err != nil {
		t.Fatalf("TraceIDFromHex() error = %v", err)
	}

	spanID, err := trace.SpanIDFromHex(testSpanID)
	if err != nil {
		t.Fatalf("SpanIDFromHex() error = %v", err)
	}

	return trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	}))
}

// decodeLine parses the single JSON record the buffer holds.
func decodeLine(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()

	entries := decodeLines(t, buf)
	if len(entries) != 1 {
		t.Fatalf("log = %q, want a single record", buf.String())
	}

	return entries[0]
}

// decodeLines parses every JSON record the buffer holds, in order.
func decodeLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()

	text := strings.TrimSpace(buf.String())
	if text == "" {
		t.Fatal("nothing was logged")
	}

	lines := strings.Split(text, "\n")
	entries := make([]map[string]any, 0, len(lines))

	for _, line := range lines {
		var entry map[string]any

		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("unmarshal %q: %v", line, err)
		}

		entries = append(entries, entry)
	}

	return entries
}
