package relay

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode"
)

const defaultNakDelay = 5 * time.Second

type Subscription struct {
	Name  string
	Topic string
	URL   *url.URL
}

type Config struct {
	NATSURL       string
	Subscriptions []Subscription
	NakDelay      time.Duration
}

func LoadConfig(getenv func(string) string) (Config, error) {
	natsURL := getenv("NATS_URL")
	if natsURL == "" {
		return Config{}, errors.New("NATS_URL is required")
	}

	subscriptions, err := parseSubscriptions(getenv("SUBSCRIPTIONS"))
	if err != nil {
		return Config{}, fmt.Errorf("SUBSCRIPTIONS: %w", err)
	}

	nakDelay := defaultNakDelay

	if raw := getenv("NAK_DELAY"); raw != "" {
		nakDelay, err = time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("NAK_DELAY: %w", err)
		}

		if nakDelay <= 0 {
			return Config{}, fmt.Errorf("NAK_DELAY must be positive: %s", raw)
		}
	}

	return Config{NATSURL: natsURL, Subscriptions: subscriptions, NakDelay: nakDelay}, nil
}

func parseSubscriptions(raw string) ([]Subscription, error) {
	var entries []struct {
		Name  string `json:"name"`
		Topic string `json:"topic"`
		URL   string `json:"url"`
	}

	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&entries); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}

	if len(entries) == 0 {
		return nil, errors.New("at least one subscription is required")
	}

	subscriptions := make([]Subscription, 0, len(entries))
	seen := make(map[string]bool, len(entries))

	for _, entry := range entries {
		if !validJetStreamName(entry.Name) {
			return nil, fmt.Errorf("name %q is not a valid JetStream durable name", entry.Name)
		}

		if seen[entry.Name] {
			return nil, fmt.Errorf("name %q is used more than once", entry.Name)
		}

		seen[entry.Name] = true

		if !validJetStreamName(entry.Topic) {
			return nil, fmt.Errorf("%s: topic %q is not a valid JetStream stream name", entry.Name, entry.Topic)
		}

		target, err := url.Parse(entry.URL)
		if err != nil || (target.Scheme != "http" && target.Scheme != "https") || target.Host == "" {
			return nil, fmt.Errorf("%s: url %q is not an absolute http or https URL", entry.Name, entry.URL)
		}

		subscriptions = append(subscriptions, Subscription{Name: entry.Name, Topic: entry.Topic, URL: target})
	}

	return subscriptions, nil
}

func validJetStreamName(name string) bool {
	return name != "" && !strings.ContainsFunc(name, func(r rune) bool {
		return unicode.IsSpace(r) || !unicode.IsPrint(r) || strings.ContainsRune(`.*>/\`, r)
	})
}
