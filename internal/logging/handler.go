// Package logging builds the slog handler the service logs through.
//
// Records are written as one JSON object per line, in the shape Cloud Logging
// parses out of a container's stdout: the severity, message and timestamp use
// the keys it reads, and the trace of the request the record belongs to is
// attached with the logging.googleapis.com/* fields it correlates on. Nothing
// here is specific to Google Cloud at run time, so the same output stays
// readable as plain JSON elsewhere.
//
// Those keys are reserved: a top-level attribute named after one of them (time,
// severity, message, msg, level, source, or anything under
// logging.googleapis.com/) would take its place in the JSON object and hide the
// real value. Such an attribute is written with an attr_ prefix instead, so both
// survive. Inside a group the names collide with nothing and are left alone.
package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/trace"
)

// LevelCritical is the level that maps to Cloud Logging's CRITICAL severity.
// It sits above slog.LevelError, which maps to ERROR.
const LevelCritical = slog.LevelError + 4

// The keys Cloud Logging reads out of a structured entry. The first three take
// the place of slog's own time, level and message keys; the rest are the
// special-purpose fields it recognises inside the JSON payload.
const (
	cloudLoggingPrefix = "logging.googleapis.com/"

	keyTime           = "time"
	keySeverity       = "severity"
	keyMessage        = "message"
	keySourceLocation = cloudLoggingPrefix + "sourceLocation"
	keyTrace          = cloudLoggingPrefix + "trace"
	keySpanID         = cloudLoggingPrefix + "spanId"
	keyTraceSampled   = cloudLoggingPrefix + "trace_sampled"
)

// reservedPrefix marks an attribute that was renamed out of the way of a key
// this handler writes itself.
const reservedPrefix = "attr_"

// reservedKeys are the top-level keys of a record. An attribute of the same
// name would replace the value the handler wrote there, so it is renamed
// instead. Everything under cloudLoggingPrefix is reserved as well.
var reservedKeys = map[string]struct{}{
	keyTime:         {}, // also slog.TimeKey
	keySeverity:     {},
	keyMessage:      {},
	slog.LevelKey:   {},
	slog.MessageKey: {},
	slog.SourceKey:  {},
}

// Options configures the handler NewHandler builds.
type Options struct {
	// Level is the lowest level that is logged. A nil Leveler means
	// slog.LevelInfo.
	Level slog.Leveler
	// AddSource records the file, line and function of each call site, at the
	// cost of a stack lookup per record.
	AddSource bool
	// ProjectID is the Google Cloud project the trace resource name is built
	// from. It may be empty; the bare trace ID is logged then, which Cloud
	// Logging cannot correlate with a trace.
	ProjectID string
}

// NewHandler returns a handler that writes Cloud Logging compatible JSON
// records to w and adds the trace of each record's context.
func NewHandler(w io.Writer, opts Options) slog.Handler {
	inner := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level:       opts.Level,
		AddSource:   opts.AddSource,
		ReplaceAttr: replaceAttr,
	})

	return newTraceHandler(inner, nil, opts.ProjectID)
}

// NewLogger returns a logger over NewHandler(w, opts).
func NewLogger(w io.Writer, opts Options) *slog.Logger {
	return slog.New(NewHandler(w, opts))
}

// ParseLevel reads a configured level name, accepting both slog's names and the
// Cloud Logging severities this package emits, in any case.
func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	case "critical":
		return LevelCritical, nil
	default:
		return 0, fmt.Errorf("unknown log level %q", s)
	}
}

// replaceAttr renames the built-in attributes of a record to the keys Cloud
// Logging reads. Only the record's own attributes are renamed: inside a group
// the same key means something the caller chose, not a built-in.
func replaceAttr(groups []string, a slog.Attr) slog.Attr {
	if len(groups) != 0 {
		return a
	}

	switch a.Key {
	case slog.TimeKey:
		// The key is reserved, but a caller can still reach this branch through
		// a group-free handler built elsewhere, and Value.Time panics on
		// anything but a time.
		if a.Value.Kind() != slog.KindTime {
			return a
		}

		return slog.String(keyTime, a.Value.Time().UTC().Format(time.RFC3339Nano))
	case slog.LevelKey:
		level, ok := a.Value.Any().(slog.Level)
		if !ok {
			return a
		}

		return slog.String(keySeverity, severityOf(level))
	case slog.MessageKey:
		return slog.String(keyMessage, a.Value.String())
	case slog.SourceKey:
		source, ok := a.Value.Any().(*slog.Source)
		if !ok {
			return a
		}

		return slog.Attr{Key: keySourceLocation, Value: slog.GroupValue(
			slog.String("file", source.File),
			// Cloud Logging types the line number as a string.
			slog.String("line", strconv.Itoa(source.Line)),
			slog.String("function", source.Function),
		)}
	default:
		return a
	}
}

// severityOf maps a level onto the Cloud Logging severity that covers it. The
// severities in between (NOTICE, ALERT, EMERGENCY) have no slog counterpart and
// are not emitted.
func severityOf(level slog.Level) string {
	switch {
	case level < slog.LevelInfo:
		return "DEBUG"
	case level < slog.LevelWarn:
		return "INFO"
	case level < slog.LevelError:
		return "WARNING"
	case level < LevelCritical:
		return "ERROR"
	default:
		return "CRITICAL"
	}
}

// isReserved reports whether a top-level attribute of this key would take the
// place of one of the keys the handler writes itself.
func isReserved(key string) bool {
	if strings.HasPrefix(key, cloudLoggingPrefix) {
		return true
	}

	_, ok := reservedKeys[key]

	return ok
}

// renameReserved moves a colliding attribute out of the way, so that it still
// reaches the entry under a name of its own.
func renameReserved(a slog.Attr) slog.Attr {
	if !isReserved(a.Key) {
		return a
	}

	return slog.Attr{Key: reservedPrefix + a.Key, Value: a.Value}
}

// renameReservedAttrs returns attrs with the colliding keys renamed, keeping the
// original slice when there is nothing to rename.
func renameReservedAttrs(attrs []slog.Attr) []slog.Attr {
	if !slices.ContainsFunc(attrs, func(a slog.Attr) bool { return isReserved(a.Key) }) {
		return attrs
	}

	renamed := make([]slog.Attr, len(attrs))
	for i, a := range attrs {
		renamed[i] = renameReserved(a)
	}

	return renamed
}

// renameReservedRecord returns record with the colliding keys of its own
// attributes renamed, rebuilding it only when there is something to rename.
func renameReservedRecord(record slog.Record) slog.Record {
	collides := false

	record.Attrs(func(a slog.Attr) bool {
		collides = isReserved(a.Key)

		return !collides
	})

	if !collides {
		return record
	}

	renamed := slog.NewRecord(record.Time, record.Level, record.Message, record.PC)

	record.Attrs(func(a slog.Attr) bool {
		renamed.AddAttrs(renameReserved(a))

		return true
	})

	return renamed
}

// traceHandler adds the trace fields of a record's context to what the inner
// handler writes, so that an entry and the span it happened in line up in Cloud
// Logging without any call site passing the trace itself.
//
// Cloud Logging only reads those fields as top-level keys, so the handler keeps
// the ungrouped base handler alongside the WithAttrs and WithGroup calls made
// on it, and adds the trace fields to the base rather than to whatever group is
// open. It keeps the derived handler ready as well, for Enabled and for the
// records that carry no span.
type traceHandler struct {
	base      slog.Handler
	inner     slog.Handler
	ops       []handlerOp
	projectID string
	// grouped records whether a group is open, in which case an attribute
	// collides with nothing at the top level and keeps its name.
	grouped bool
}

// handlerOp is one WithAttrs or WithGroup call made on the base handler. A
// non-empty group names a WithGroup call; slog gives an empty group name no
// effect, so the two never collide.
type handlerOp struct {
	attrs []slog.Attr
	group string
}

func (o handlerOp) apply(handler slog.Handler) slog.Handler {
	if o.group != "" {
		return handler.WithGroup(o.group)
	}

	return handler.WithAttrs(o.attrs)
}

// newTraceHandler applies ops to base once, so that the common path of a record
// without a span logs through a handler that is already built.
func newTraceHandler(base slog.Handler, ops []handlerOp, projectID string) *traceHandler {
	inner := base
	grouped := false

	for _, op := range ops {
		inner = op.apply(inner)
		grouped = grouped || op.group != ""
	}

	return &traceHandler{base: base, inner: inner, ops: ops, projectID: projectID, grouped: grouped}
}

func (h *traceHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *traceHandler) Handle(ctx context.Context, record slog.Record) error {
	if !h.grouped {
		record = renameReservedRecord(record)
	}

	spanContext := trace.SpanFromContext(ctx).SpanContext()
	if !spanContext.IsValid() {
		return h.inner.Handle(ctx, record)
	}

	// The trace fields go onto the base handler and the groups and attributes
	// of this handler are replayed over them, which puts the fields at the top
	// level and leaves the record's own attributes inside the open group.
	// Rebuilding the chain costs a record's worth of work, which is acceptable
	// because only the records made under an active span pay it.
	handler := h.base.WithAttrs([]slog.Attr{
		slog.String(keyTrace, h.traceResourceName(spanContext.TraceID().String())),
		slog.String(keySpanID, spanContext.SpanID().String()),
		slog.Bool(keyTraceSampled, spanContext.IsSampled()),
	})

	for _, op := range h.ops {
		handler = op.apply(handler)
	}

	return handler.Handle(ctx, record)
}

func (h *traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}

	if !h.grouped {
		attrs = renameReservedAttrs(attrs)
	}

	return newTraceHandler(h.base, h.appendOp(handlerOp{attrs: attrs, group: ""}), h.projectID)
}

func (h *traceHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}

	return newTraceHandler(h.base, h.appendOp(handlerOp{attrs: nil, group: name}), h.projectID)
}

// appendOp copies the recorded operations before appending, so that two
// handlers derived from the same one never share backing storage.
func (h *traceHandler) appendOp(op handlerOp) []handlerOp {
	ops := make([]handlerOp, 0, len(h.ops)+1)
	ops = append(ops, h.ops...)

	return append(ops, op)
}

// traceResourceName names the trace the way Cloud Logging resolves it. The
// project is part of that name, so without one the entry carries the bare trace
// ID and can be matched by eye but not by Cloud Logging itself.
func (h *traceHandler) traceResourceName(traceID string) string {
	if h.projectID == "" {
		return traceID
	}

	return "projects/" + h.projectID + "/traces/" + traceID
}
