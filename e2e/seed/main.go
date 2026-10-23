//go:build e2e

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-nats/v2/pkg/jetstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/nats.go"
	natsjetstream "github.com/nats-io/nats.go/jetstream"
)

const (
	natsURL = "nats://nats:4222"
	topic   = "e2e-deliver-new"
)

func main() {
	if err := seed(context.Background()); err != nil {
		slog.Error("seeding the stream failed", "error", err)
		os.Exit(1)
	}
}

func seed(ctx context.Context) error {
	conn, err := nats.Connect(natsURL)
	if err != nil {
		return fmt.Errorf("connect to NATS: %w", err)
	}
	defer conn.Close()

	js, err := natsjetstream.New(conn)
	if err != nil {
		return fmt.Errorf("create JetStream context: %w", err)
	}

	if _, err := js.CreateOrUpdateStream(ctx, natsjetstream.StreamConfig{
		Name:     topic,
		Subjects: []string{topic},
	}); err != nil {
		return fmt.Errorf("provision the %s stream: %w", topic, err)
	}

	publisher, err := jetstream.NewPublisher(jetstream.PublisherConfig{
		URL:             "",
		Conn:            conn,
		Logger:          watermill.NewSlogLogger(slog.Default()),
		ConfigureStream: nil,
		TrackMessageID:  false,
	})
	if err != nil {
		return fmt.Errorf("create NATS JetStream publisher: %w", err)
	}

	if err := publisher.Publish(topic, message.NewMessage(watermill.NewUUID(), []byte("before"))); err != nil {
		return fmt.Errorf("publish to %s: %w", topic, err)
	}

	return nil
}
