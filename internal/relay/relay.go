package relay

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-nats/v2/pkg/jetstream"
	wmnats "github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/nats.go"
	natsjetstream "github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/pj-hoakari/jetstream-http-push/internal/infra/httpapi"
)

const (
	deliveryTimeout = 10 * time.Second
	handlerTimeout  = 25 * time.Second
	ackWait         = 30 * time.Second

	eventIDMetadataKey     = "event_id"
	publishTimeMetadataKey = "jetstream_publish_time"

	tracerName = "github.com/pj-hoakari/jetstream-http-push/internal/relay"
)

var traceContextHeaders = []string{"traceparent", "tracestate", "baggage"}

type Relay struct {
	router *message.Router
	conns  []*nats.Conn
}

func New(cfg Config) (*Relay, error) {
	wmLogger := watermill.NewSlogLogger(slog.Default())

	router, err := message.NewRouter(message.RouterConfig{}, wmLogger)
	if err != nil {
		return nil, fmt.Errorf("create router: %w", err)
	}

	relay := &Relay{router: router, conns: nil}
	client := newClient()

	for _, sub := range cfg.Subscriptions {
		conn, err := nats.Connect(cfg.NATSURL, nats.MaxReconnects(-1))
		if err != nil {
			relay.closeConns()

			return nil, fmt.Errorf("%s: connect to NATS: %w", sub.Name, err)
		}

		relay.conns = append(relay.conns, conn)

		subscriber, err := jetstream.NewSubscriber(jetstream.SubscriberConfig{
			Conn:                conn,
			Logger:              wmLogger,
			AckWaitTimeout:      handlerTimeout,
			ResourceInitializer: durableConsumer(sub.Name),
			NakDelay:            jetstream.NewStaticDelay(cfg.NakDelay),
			Unmarshaler:         unmarshaler{},
		})
		if err != nil {
			relay.closeConns()

			return nil, fmt.Errorf("%s: create subscriber: %w", sub.Name, err)
		}

		router.AddConsumerHandler(sub.Name, sub.Topic, subscriber, deliverTo(client, sub))
	}

	return relay, nil
}

func (r *Relay) Run(ctx context.Context) error {
	if err := r.router.Run(ctx); err != nil {
		r.closeConns()

		return fmt.Errorf("run router: %w", err)
	}

	return nil
}

func (r *Relay) ReadinessChecks() []httpapi.ReadinessCheck {
	return readinessChecks(r.router, r.conns)
}

func (r *Relay) closeConns() {
	for _, conn := range r.conns {
		conn.Close()
	}
}

func readinessChecks(router *message.Router, conns []*nats.Conn) []httpapi.ReadinessCheck {
	return []httpapi.ReadinessCheck{
		{Name: "router", Check: func(context.Context) error {
			if !router.IsRunning() || router.IsClosed() {
				return errors.New("router is not running")
			}

			return nil
		}},
		{Name: "nats", Check: func(context.Context) error {
			for _, conn := range conns {
				if !conn.IsConnected() {
					return errors.New("a NATS connection is not connected")
				}
			}

			return nil
		}},
	}
}

func durableConsumer(name string) jetstream.ResourceInitializer {
	return func(ctx context.Context, js natsjetstream.JetStream, topic string) (
		natsjetstream.Consumer, func(context.Context, watermill.LoggerAdapter), error,
	) {
		stream, err := js.CreateStream(ctx, natsjetstream.StreamConfig{Name: topic, Subjects: []string{topic}})
		if errors.Is(err, natsjetstream.ErrStreamNameAlreadyInUse) {
			stream, err = js.Stream(ctx, topic)
		}

		if err != nil {
			return nil, nil, fmt.Errorf("ensure the %s stream: %w", topic, err)
		}

		consumer, err := stream.CreateOrUpdateConsumer(ctx, natsjetstream.ConsumerConfig{
			Name:          name,
			Durable:       name,
			DeliverPolicy: natsjetstream.DeliverNewPolicy,
			AckPolicy:     natsjetstream.AckExplicitPolicy,
			AckWait:       ackWait,
			MaxAckPending: 1,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("ensure the %s consumer on %s: %w", name, topic, err)
		}

		return consumer, nil, nil
	}
}

type unmarshaler struct{}

func (unmarshaler) Unmarshal(msg natsjetstream.Msg) (*message.Message, error) {
	id := msg.Headers().Get(wmnats.WatermillUUIDHdr)
	metadata := message.Metadata{eventIDMetadataKey: msg.Headers().Get(eventIDMetadataKey)}

	for _, key := range traceContextHeaders {
		if value := msg.Headers().Get(key); value != "" {
			metadata.Set(key, value)
		}
	}

	if meta, err := msg.Metadata(); err == nil {
		metadata.Set(publishTimeMetadataKey, meta.Timestamp.UTC().Format(time.RFC3339Nano))

		if id == "" {
			id = strconv.FormatUint(meta.Sequence.Stream, 10)
		}
	}

	m := message.NewMessage(id, msg.Data())
	m.Metadata = metadata

	return m, nil
}

func deliverTo(client *http.Client, sub Subscription) message.NoPublishHandlerFunc {
	return func(msg *message.Message) error {
		parent := otel.GetTextMapPropagator().Extract(msg.Context(), propagation.MapCarrier(msg.Metadata))

		ctx, span := otel.Tracer(tracerName).Start(parent, "process "+sub.Topic,
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(
				semconv.MessagingSystemKey.String("nats"),
				semconv.MessagingOperationTypeProcess,
				semconv.MessagingDestinationName(sub.Topic),
				semconv.MessagingConsumerGroupName(sub.Name),
				semconv.MessagingMessageID(msg.UUID),
				semconv.URLFull(sub.URL.Redacted()),
			),
		)
		defer span.End()

		if err := post(ctx, client, sub.URL, msg); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())

			return err
		}

		return nil
	}
}

func post(ctx context.Context, client *http.Client, target *url.URL, msg *message.Message) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(msg.Payload))
	if err != nil {
		return fmt.Errorf("build the request to %s: %w", target, err)
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Goog-Pubsub-Message-Id", msg.UUID)
	req.Header.Set("X-Goog-Pubsub-Publish-Time", msg.Metadata.Get(publishTimeMetadataKey))

	if key := msg.Metadata.Get(eventIDMetadataKey); key != "" {
		req.Header.Set("X-Goog-Pubsub-Ordering-Key", key)
	}

	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("post to %s: %w", target, err)
	}

	trace.SpanFromContext(ctx).SetAttributes(semconv.HTTPResponseStatusCode(resp.StatusCode))

	defer func() {
		if err := resp.Body.Close(); err != nil {
			slog.WarnContext(ctx, "closing the response body failed", "url", target.String(), "error", err)
		}
	}()

	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		slog.WarnContext(ctx, "draining the response body failed", "url", target.String(), "error", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("%s responded %s", target, resp.Status)
	}

	return nil
}

func newClient() *http.Client {
	return &http.Client{
		Timeout: deliveryTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}
