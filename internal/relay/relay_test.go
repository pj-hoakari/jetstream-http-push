package relay

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/nats-io/nats.go"
	natsjetstream "github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/pj-hoakari/jetstream-http-push/internal/infra/httpapi"
)

type jetStreamMsg struct {
	natsjetstream.Msg
	data     []byte
	headers  nats.Header
	metadata *natsjetstream.MsgMetadata
}

func (m jetStreamMsg) Data() []byte         { return m.data }
func (m jetStreamMsg) Headers() nats.Header { return m.headers }

func (m jetStreamMsg) Metadata() (*natsjetstream.MsgMetadata, error) { return m.metadata, nil }

type received struct {
	method string
	path   string
	header http.Header
	body   []byte
}

func receiver(t *testing.T, status int) (*url.URL, <-chan received) {
	t.Helper()

	got := make(chan received, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got <- received{method: r.Method, path: r.URL.Path, header: r.Header, body: body}

		w.WriteHeader(status)
	}))
	t.Cleanup(server.Close)

	target, err := url.Parse(server.URL + "/push/guest-status")
	if err != nil {
		t.Fatal(err)
	}

	return target, got
}

func relayed(t *testing.T, msg natsjetstream.Msg, target *url.URL) error {
	t.Helper()

	m, err := unmarshaler{}.Unmarshal(msg)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	return deliverTo(newClient(), subscription(target))(m)
}

func subscription(target *url.URL) Subscription {
	return Subscription{Name: "guest-status-push", Topic: "guest-status", URL: target}
}

func TestDelivery(t *testing.T) {
	t.Parallel()

	jst := time.FixedZone("JST", 9*60*60)
	payload := []byte{0x00, 0xff, 0x0a, 'p', 0x80}

	t.Run("posts the payload bytes with the push headers", func(t *testing.T) {
		t.Parallel()

		target, got := receiver(t, http.StatusOK)

		err := relayed(t, jetStreamMsg{
			data:     payload,
			headers:  nats.Header{"_watermill_message_uuid": {"uuid-1"}, "event_id": {"event-1"}},
			metadata: &natsjetstream.MsgMetadata{Timestamp: time.Date(2026, 10, 23, 12, 0, 0, 123456789, jst), Sequence: natsjetstream.SequencePair{Stream: 42}},
		}, target)
		if err != nil {
			t.Fatalf("delivery error = %v, want nil", err)
		}

		r := <-got
		if r.method != http.MethodPost || r.path != "/push/guest-status" {
			t.Errorf("request = %s %s, want POST /push/guest-status", r.method, r.path)
		}

		if !bytes.Equal(r.body, payload) {
			t.Errorf("body = %x, want %x", r.body, payload)
		}

		for name, want := range map[string]string{
			"Content-Type":               "application/octet-stream",
			"x-goog-pubsub-message-id":   "uuid-1",
			"x-goog-pubsub-ordering-key": "event-1",
			"x-goog-pubsub-publish-time": "2026-10-23T03:00:00.123456789Z",
		} {
			if got := r.header.Get(name); got != want {
				t.Errorf("%s = %q, want %q", name, got, want)
			}
		}
	})

	t.Run("omits the ordering key when the message has no event_id", func(t *testing.T) {
		t.Parallel()

		target, got := receiver(t, http.StatusOK)

		err := relayed(t, jetStreamMsg{
			data:     payload,
			headers:  nats.Header{"_watermill_message_uuid": {"uuid-1"}},
			metadata: &natsjetstream.MsgMetadata{Timestamp: time.Now(), Sequence: natsjetstream.SequencePair{Stream: 42}},
		}, target)
		if err != nil {
			t.Fatalf("delivery error = %v, want nil", err)
		}

		if values := (<-got).header.Values("x-goog-pubsub-ordering-key"); len(values) != 0 {
			t.Errorf("x-goog-pubsub-ordering-key = %q, want no header", values)
		}
	})

	t.Run("uses the stream sequence as the message id when the message has no watermill UUID", func(t *testing.T) {
		t.Parallel()

		target, got := receiver(t, http.StatusOK)

		err := relayed(t, jetStreamMsg{
			data:     payload,
			headers:  nats.Header{"event_id": {"event-1"}},
			metadata: &natsjetstream.MsgMetadata{Timestamp: time.Now(), Sequence: natsjetstream.SequencePair{Stream: 42, Consumer: 7}},
		}, target)
		if err != nil {
			t.Fatalf("delivery error = %v, want nil", err)
		}

		if id := (<-got).header.Get("x-goog-pubsub-message-id"); id != "42" {
			t.Errorf("x-goog-pubsub-message-id = %q, want the stream sequence 42", id)
		}
	})

	for status, wantAck := range map[int]bool{
		http.StatusOK:                  true,
		http.StatusNoContent:           true,
		http.StatusFound:               false,
		http.StatusNotFound:            false,
		http.StatusInternalServerError: false,
		http.StatusServiceUnavailable:  false,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Location", "/elsewhere")
				w.WriteHeader(status)
			}))
			t.Cleanup(server.Close)

			target, _ := url.Parse(server.URL)
			msg := message.NewMessage("uuid-1", payload)

			err := deliverTo(newClient(), subscription(target))(msg)
			if acked := err == nil; acked != wantAck {
				t.Errorf("status %d: error = %v, want ack = %v", status, err, wantAck)
			}
		})
	}

	t.Run("an unreachable endpoint is a failure", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.NotFoundHandler())
		target, _ := url.Parse(server.URL)
		server.Close()

		if err := deliverTo(newClient(), subscription(target))(message.NewMessage("uuid-1", payload)); err == nil {
			t.Error("error = nil, want a transport error")
		}
	})
}

func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()

	recorder := tracetest.NewSpanRecorder()
	provider, propagator := otel.GetTracerProvider(), otel.GetTextMapPropagator()

	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(provider)
		otel.SetTextMapPropagator(propagator)
	})

	return recorder
}

func onlySpan(t *testing.T, recorder *tracetest.SpanRecorder) sdktrace.ReadOnlySpan {
	t.Helper()

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(spans))
	}

	return spans[0]
}

func TestDeliveryTracing(t *testing.T) {
	newMsg := func(headers nats.Header) jetStreamMsg {
		headers.Set("_watermill_message_uuid", "uuid-1")

		return jetStreamMsg{
			data:     []byte("payload"),
			headers:  headers,
			metadata: &natsjetstream.MsgMetadata{Timestamp: time.Now(), Sequence: natsjetstream.SequencePair{Stream: 42}},
		}
	}

	t.Run("records one consumer span for the delivery attempt", func(t *testing.T) {
		recorder := recordSpans(t)
		target, _ := receiver(t, http.StatusOK)

		if err := relayed(t, newMsg(nats.Header{}), target); err != nil {
			t.Fatalf("delivery error = %v, want nil", err)
		}

		span := onlySpan(t, recorder)

		if span.Name() != "process guest-status" || span.SpanKind() != trace.SpanKindConsumer {
			t.Errorf("span = %q (%s), want process guest-status (consumer)", span.Name(), span.SpanKind())
		}

		attrs := attribute.NewSet(span.Attributes()...)
		for key, want := range map[attribute.Key]string{
			"messaging.system":              "nats",
			"messaging.operation.type":      "process",
			"messaging.destination.name":    "guest-status",
			"messaging.consumer.group.name": "guest-status-push",
			"messaging.message.id":          "uuid-1",
			"url.full":                      target.String(),
			"http.response.status_code":     "200",
		} {
			if got, ok := attrs.Value(key); !ok || got.Emit() != want {
				t.Errorf("%s = %q (set %v), want %q", key, got.Emit(), ok, want)
			}
		}

		if span.Status().Code != codes.Unset {
			t.Errorf("status = %v, want unset", span.Status())
		}

		if span.Parent().IsValid() {
			t.Errorf("parent = %v, want a root span for a message without a traceparent", span.Parent())
		}
	})

	t.Run("marks the span as an error when the receiver does not return 2xx", func(t *testing.T) {
		recorder := recordSpans(t)
		target, _ := receiver(t, http.StatusInternalServerError)

		if err := relayed(t, newMsg(nats.Header{}), target); err == nil {
			t.Fatal("delivery error = nil, want the 500 as an error")
		}

		span := onlySpan(t, recorder)

		if span.Status().Code != codes.Error {
			t.Errorf("status = %v, want error", span.Status())
		}

		attrs := attribute.NewSet(span.Attributes()...)
		if got, _ := attrs.Value("http.response.status_code"); got.AsInt64() != http.StatusInternalServerError {
			t.Errorf("http.response.status_code = %v, want 500", got.Emit())
		}
	})

	t.Run("continues the trace of the message and passes it to the receiver", func(t *testing.T) {
		const traceID, parentSpanID = "0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331"

		recorder := recordSpans(t)
		target, got := receiver(t, http.StatusOK)

		msg := newMsg(nats.Header{"traceparent": {"00-" + traceID + "-" + parentSpanID + "-01"}})
		if err := relayed(t, msg, target); err != nil {
			t.Fatalf("delivery error = %v, want nil", err)
		}

		span := onlySpan(t, recorder)

		if span.SpanContext().TraceID().String() != traceID || span.Parent().SpanID().String() != parentSpanID {
			t.Errorf("span trace %s, parent %s, want trace %s, parent %s",
				span.SpanContext().TraceID(), span.Parent().SpanID(), traceID, parentSpanID)
		}

		want := "00-" + traceID + "-" + span.SpanContext().SpanID().String() + "-01"
		if traceparent := (<-got).header.Get("traceparent"); traceparent != want {
			t.Errorf("receiver traceparent = %q, want %q", traceparent, want)
		}
	})
}

func TestReadyz(t *testing.T) {
	t.Parallel()

	readyz := func(router *message.Router, conns []*nats.Conn) int {
		rec := httptest.NewRecorder()
		httpapi.NewHandler(httpapi.HealthRoutes(readinessChecks(router, conns)...)).
			ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

		return rec.Code
	}

	logger := watermill.NopLogger{}

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		t.Fatal(err)
	}

	pubSub := gochannel.NewGoChannel(gochannel.Config{}, logger)
	router.AddConsumerHandler("handler", "topic", pubSub, func(*message.Message) error { return nil })

	if got := readyz(router, nil); got != http.StatusServiceUnavailable {
		t.Errorf("before the router runs: readyz = %d, want 503", got)
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error)

	go func() { done <- router.Run(ctx) }()

	<-router.Running()

	if got := readyz(router, nil); got != http.StatusOK {
		t.Errorf("router running and every connection up: readyz = %d, want 200", got)
	}

	if got := readyz(router, []*nats.Conn{{}}); got != http.StatusServiceUnavailable {
		t.Errorf("router running and a connection down: readyz = %d, want 503", got)
	}

	cancel()

	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got := readyz(router, nil); got != http.StatusServiceUnavailable {
		t.Errorf("after the router closed: readyz = %d, want 503", got)
	}
}
