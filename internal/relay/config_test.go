package relay

import (
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	env := func(overrides map[string]string) func(string) string {
		values := map[string]string{
			"NATS_URL":      "nats://nats:4222",
			"SUBSCRIPTIONS": `[{"name": "guest-status-to-guest", "topic": "guest-status", "url": "http://guest:8080/push/guest-status"}]`,
		}
		for k, v := range overrides {
			values[k] = v
		}

		return func(key string) string { return values[key] }
	}

	t.Run("valid", func(t *testing.T) {
		t.Parallel()

		cfg, err := LoadConfig(env(map[string]string{
			"SUBSCRIPTIONS": `[
				{"name": "a", "topic": "guest-status", "url": "http://guest:8080/push"},
				{"name": "b", "topic": "guest-status", "url": "https://realtime.example/push"}
			]`,
		}))
		if err != nil {
			t.Fatalf("LoadConfig() error = %v", err)
		}

		if cfg.NATSURL != "nats://nats:4222" {
			t.Errorf("NATSURL = %q", cfg.NATSURL)
		}

		if cfg.NakDelay != 5*time.Second {
			t.Errorf("NakDelay = %v, want the 5s default", cfg.NakDelay)
		}

		want := []struct{ name, topic, url string }{
			{"a", "guest-status", "http://guest:8080/push"},
			{"b", "guest-status", "https://realtime.example/push"},
		}
		if len(cfg.Subscriptions) != len(want) {
			t.Fatalf("Subscriptions = %+v, want %d entries", cfg.Subscriptions, len(want))
		}

		for i, w := range want {
			got := cfg.Subscriptions[i]
			if got.Name != w.name || got.Topic != w.topic || got.URL.String() != w.url {
				t.Errorf("Subscriptions[%d] = {%s %s %s}, want %+v", i, got.Name, got.Topic, got.URL, w)
			}
		}
	})

	t.Run("NAK_DELAY overrides the default", func(t *testing.T) {
		t.Parallel()

		cfg, err := LoadConfig(env(map[string]string{"NAK_DELAY": "200ms"}))
		if err != nil {
			t.Fatalf("LoadConfig() error = %v", err)
		}

		if cfg.NakDelay != 200*time.Millisecond {
			t.Errorf("NakDelay = %v, want 200ms", cfg.NakDelay)
		}
	})

	rejected := map[string]map[string]string{
		"missing NATS_URL":      {"NATS_URL": ""},
		"missing SUBSCRIPTIONS": {"SUBSCRIPTIONS": ""},
		"malformed JSON":        {"SUBSCRIPTIONS": `[{"name": "a"`},
		"empty list":            {"SUBSCRIPTIONS": `[]`},
		"null list":             {"SUBSCRIPTIONS": `null`},
		"unknown field":         {"SUBSCRIPTIONS": `[{"name": "a", "topic": "t", "url": "http://h/p", "group": "g"}]`},
		"empty name":            {"SUBSCRIPTIONS": `[{"name": "", "topic": "t", "url": "http://h/p"}]`},
		"duplicate name":        {"SUBSCRIPTIONS": `[{"name": "a", "topic": "t", "url": "http://h/p"}, {"name": "a", "topic": "u", "url": "http://h/q"}]`},
		"name with a dot":       {"SUBSCRIPTIONS": `[{"name": "a.b", "topic": "t", "url": "http://h/p"}]`},
		"name with a wildcard":  {"SUBSCRIPTIONS": `[{"name": "a*", "topic": "t", "url": "http://h/p"}]`},
		"name with a gt":        {"SUBSCRIPTIONS": `[{"name": "a>", "topic": "t", "url": "http://h/p"}]`},
		"name with a space":     {"SUBSCRIPTIONS": `[{"name": "a b", "topic": "t", "url": "http://h/p"}]`},
		"name with a slash":     {"SUBSCRIPTIONS": `[{"name": "a/b", "topic": "t", "url": "http://h/p"}]`},
		"name with a backslash": {"SUBSCRIPTIONS": `[{"name": "a\\b", "topic": "t", "url": "http://h/p"}]`},
		"empty topic":           {"SUBSCRIPTIONS": `[{"name": "a", "topic": "", "url": "http://h/p"}]`},
		"topic with a dot":      {"SUBSCRIPTIONS": `[{"name": "a", "topic": "t.u", "url": "http://h/p"}]`},
		"relative url":          {"SUBSCRIPTIONS": `[{"name": "a", "topic": "t", "url": "/push"}]`},
		"url without a host":    {"SUBSCRIPTIONS": `[{"name": "a", "topic": "t", "url": "http:///push"}]`},
		"non-http url":          {"SUBSCRIPTIONS": `[{"name": "a", "topic": "t", "url": "nats://h:4222"}]`},
		"unparsable url":        {"SUBSCRIPTIONS": `[{"name": "a", "topic": "t", "url": "http://h:port/p"}]`},
		"unparsable NAK_DELAY":  {"NAK_DELAY": "5"},
		"zero NAK_DELAY":        {"NAK_DELAY": "0s"},
		"negative NAK_DELAY":    {"NAK_DELAY": "-1ns"},
	}
	for name, overrides := range rejected {
		t.Run(name+" is rejected", func(t *testing.T) {
			t.Parallel()

			if cfg, err := LoadConfig(env(overrides)); err == nil {
				t.Errorf("LoadConfig() = %+v, want an error", cfg)
			}
		})
	}
}
