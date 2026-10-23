//go:build e2e

package e2e

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-nats/v2/pkg/jetstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/nats.go"
	natsjetstream "github.com/nats-io/nats.go/jetstream"
)

const (
	natsURL      = "nats://nats:4222"
	readyzURL    = "http://relay:8080/readyz"
	receiverAddr = ":8080"
	waitTimeout  = 10 * time.Second
)

var receiver = http.NewServeMux()

func TestMain(m *testing.M) {
	go func() {
		if err := http.ListenAndServe(receiverAddr, receiver); err != nil {
			fmt.Fprintln(os.Stderr, "receiver stopped:", err)
			os.Exit(1)
		}
	}()

	if err := waitForReady(time.Minute); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

func TestRelay(t *testing.T) {
	t.Run("中継は全購読の準備ができると readyz が 200 を返す", func(t *testing.T) {
		t.Parallel()

		if status := readyzStatus(t); status != http.StatusOK {
			t.Fatalf("readyz = %d, want 200", status)
		}
	})

	t.Run("本文は publish したペイロードのバイト列のまま届き、3 つのヘッダーが付く", func(t *testing.T) {
		t.Parallel()

		deliveries := endpoint(t, "/delivery", alwaysRespond(http.StatusOK))
		payload := []byte{0x00, 0xff, 0x0a, 'p', 0x80}
		id := publish(t, "e2e-delivery", payload, "event-delivery")

		got := next(t, deliveries)

		if !bytes.Equal(got.body, payload) {
			t.Errorf("body = %x, want %x", got.body, payload)
		}

		if got.contentType != "application/octet-stream" {
			t.Errorf("Content-Type = %q, want application/octet-stream", got.contentType)
		}

		if got.id != id {
			t.Errorf("x-goog-pubsub-message-id = %q, want the published UUID %q", got.id, id)
		}

		if got.orderingKey != "event-delivery" {
			t.Errorf("x-goog-pubsub-ordering-key = %q, want the event_id event-delivery", got.orderingKey)
		}

		publishTime, err := time.Parse(time.RFC3339Nano, got.publishTime)
		if err != nil {
			t.Fatalf("x-goog-pubsub-publish-time = %q, want RFC 3339: %v", got.publishTime, err)
		}

		if publishTime.Location() != time.UTC {
			t.Errorf("x-goog-pubsub-publish-time = %q, want UTC", got.publishTime)
		}

		if age := time.Since(publishTime); age < -time.Minute || age > time.Minute {
			t.Errorf("x-goog-pubsub-publish-time = %q, want it close to now", got.publishTime)
		}
	})

	t.Run("受け口が 2xx を返したメッセージは再送されない", func(t *testing.T) {
		t.Parallel()

		deliveries := endpoint(t, "/ack", alwaysRespond(http.StatusNoContent))
		id := publish(t, "e2e-ack", []byte("acked"), "event-ack")

		if got := next(t, deliveries); got.id != id {
			t.Fatalf("message-id = %q, want %q", got.id, id)
		}

		select {
		case again := <-deliveries:
			t.Errorf("message %q was delivered again after a 2xx response", again.id)
		case <-time.After(2 * time.Second):
		}
	})

	t.Run("受け口が 2xx 以外を返したメッセージは同じ message-id で再送される", func(t *testing.T) {
		t.Parallel()

		deliveries := endpoint(t, "/redelivery", func(_ delivery, attempt int) int {
			if attempt == 1 {
				return http.StatusServiceUnavailable
			}

			return http.StatusOK
		})
		id := publish(t, "e2e-redelivery", []byte("retried"), "event-redelivery")

		first, second := next(t, deliveries), next(t, deliveries)

		if first.id != id || second.id != id {
			t.Errorf("message-ids = %q, %q, want both %q", first.id, second.id, id)
		}

		if !bytes.Equal(first.body, second.body) {
			t.Errorf("redelivered body = %q, want %q", second.body, first.body)
		}
	})

	t.Run("先のメッセージが ack されるまで、同じトピックの次のメッセージは届かない", func(t *testing.T) {
		t.Parallel()

		deliveries := endpoint(t, "/ordering", func(d delivery, attempt int) int {
			if string(d.body) == "first" && attempt <= 2 {
				return http.StatusInternalServerError
			}

			return http.StatusOK
		})
		publish(t, "e2e-ordering", []byte("first"), "event-ordering-1")
		publish(t, "e2e-ordering", []byte("second"), "event-ordering-2")

		var order []string
		for range 4 {
			order = append(order, string(next(t, deliveries).body))
		}

		if want := []string{"first", "first", "first", "second"}; fmt.Sprint(order) != fmt.Sprint(want) {
			t.Errorf("delivery order = %q, want %q", order, want)
		}
	})

	t.Run("同じトピックを購読する 2 つの宛先の両方に届く", func(t *testing.T) {
		t.Parallel()

		a := endpoint(t, "/fanout-a", alwaysRespond(http.StatusOK))
		b := endpoint(t, "/fanout-b", alwaysRespond(http.StatusOK))
		id := publish(t, "e2e-fanout", []byte("fanned out"), "event-fanout")

		for name, deliveries := range map[string]<-chan delivery{"fanout-a": a, "fanout-b": b} {
			if got := next(t, deliveries); got.id != id {
				t.Errorf("%s got message-id %q, want %q", name, got.id, id)
			}
		}
	})
}

type delivery struct {
	id          string
	orderingKey string
	publishTime string
	contentType string
	body        []byte
}

func alwaysRespond(status int) func(delivery, int) int {
	return func(delivery, int) int { return status }
}

func endpoint(t *testing.T, path string, respond func(d delivery, attempt int) int) <-chan delivery {
	t.Helper()

	deliveries := make(chan delivery, 100)
	attempts := map[string]int{}

	var mu sync.Mutex

	receiver.HandleFunc("POST "+path, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)

			return
		}

		d := delivery{
			id:          r.Header.Get("x-goog-pubsub-message-id"),
			orderingKey: r.Header.Get("x-goog-pubsub-ordering-key"),
			publishTime: r.Header.Get("x-goog-pubsub-publish-time"),
			contentType: r.Header.Get("Content-Type"),
			body:        body,
		}

		mu.Lock()
		attempts[d.id]++
		attempt := attempts[d.id]
		mu.Unlock()

		deliveries <- d

		w.WriteHeader(respond(d, attempt))
	})

	return deliveries
}

func next(t *testing.T, deliveries <-chan delivery) delivery {
	t.Helper()

	select {
	case d := <-deliveries:
		return d
	case <-time.After(waitTimeout):
		t.Fatalf("no delivery within %s", waitTimeout)

		return delivery{}
	}
}

func publish(t *testing.T, topic string, payload []byte, eventID string) string {
	t.Helper()

	conn, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("connect to NATS: %v", err)
	}

	t.Cleanup(conn.Close)

	js, err := natsjetstream.New(conn)
	if err != nil {
		t.Fatalf("create JetStream context: %v", err)
	}

	if _, err := js.CreateOrUpdateStream(t.Context(), natsjetstream.StreamConfig{
		Name:     topic,
		Subjects: []string{topic},
	}); err != nil {
		t.Fatalf("provision the %s stream: %v", topic, err)
	}

	publisher, err := jetstream.NewPublisher(jetstream.PublisherConfig{
		URL:             "",
		Conn:            conn,
		Logger:          watermill.NewSlogLogger(slog.Default()),
		ConfigureStream: nil,
		TrackMessageID:  false,
	})
	if err != nil {
		t.Fatalf("create NATS JetStream publisher: %v", err)
	}

	msg := message.NewMessage(watermill.NewUUID(), payload)
	msg.Metadata.Set("event_id", eventID)

	if err := publisher.Publish(topic, msg); err != nil {
		t.Fatalf("publish to %s: %v", topic, err)
	}

	return msg.UUID
}

func waitForReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if resp, err := http.Get(readyzURL); err == nil {
			resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}

		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("%s did not return 200 within %s", readyzURL, timeout)
}

func readyzStatus(t *testing.T) int {
	t.Helper()

	resp, err := http.Get(readyzURL)
	if err != nil {
		t.Fatalf("GET %s: %v", readyzURL, err)
	}
	defer resp.Body.Close()

	return resp.StatusCode
}
