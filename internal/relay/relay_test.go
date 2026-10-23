package relay

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/nats-io/nats.go"
	natsjetstream "github.com/nats-io/nats.go/jetstream"

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

	return deliverTo(newClient(), target)(m)
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

			err := deliverTo(newClient(), target)(msg)
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

		if err := deliverTo(newClient(), target)(message.NewMessage("uuid-1", payload)); err == nil {
			t.Error("error = nil, want a transport error")
		}
	})

	t.Run("a failure does not reveal the password in the endpoint URL", func(t *testing.T) {
		t.Parallel()

		target, _ := receiver(t, http.StatusInternalServerError)
		target.User = url.UserPassword("relay", "s3cret")

		err := deliverTo(newClient(), target)(message.NewMessage("uuid-1", payload))
		if err == nil || strings.Contains(err.Error(), "s3cret") {
			t.Errorf("error = %v, want a failure without the password", err)
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
