package httpapi_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pj-hoakari/jetstream-http-push/internal/infra/httpapi"
)

func TestHealthz(t *testing.T) {
	t.Parallel()

	status, body := request(t, httpapi.NewHandler(httpapi.HealthRoutes()), http.MethodGet, "/healthz")

	if want := http.StatusOK; status != want {
		t.Fatalf("status = %d, want %d", status, want)
	}

	if want := "ok"; body != want {
		t.Errorf("body = %q, want %q", body, want)
	}
}

func TestHealthzRejectsNonGet(t *testing.T) {
	t.Parallel()

	status, _ := request(t, httpapi.NewHandler(httpapi.HealthRoutes()), http.MethodPost, "/healthz")

	if want := http.StatusMethodNotAllowed; status != want {
		t.Fatalf("status = %d, want %d", status, want)
	}
}

func TestReadyzWithoutChecks(t *testing.T) {
	t.Parallel()

	status, body := request(t, httpapi.NewHandler(httpapi.HealthRoutes()), http.MethodGet, "/readyz")

	if want := http.StatusOK; status != want {
		t.Fatalf("status = %d, want %d", status, want)
	}

	if want := "ok"; body != want {
		t.Errorf("body = %q, want %q", body, want)
	}
}

func TestReadyzWithPassingChecks(t *testing.T) {
	t.Parallel()

	handler := httpapi.NewHandler(httpapi.HealthRoutes(
		httpapi.ReadinessCheck{Name: "primary-db", Check: func(context.Context) error { return nil }},
		httpapi.ReadinessCheck{Name: "cache", Check: func(context.Context) error { return nil }},
	))

	status, body := request(t, handler, http.MethodGet, "/readyz")

	if want := http.StatusOK; status != want {
		t.Fatalf("status = %d, want %d", status, want)
	}

	if want := "ok"; body != want {
		t.Errorf("body = %q, want %q", body, want)
	}
}

func TestReadyzWithFailingCheck(t *testing.T) {
	t.Parallel()

	laterCheckRan := false
	handler := httpapi.NewHandler(httpapi.HealthRoutes(
		httpapi.ReadinessCheck{Name: "primary-db", Check: func(context.Context) error {
			return errors.New("dial tcp: connection refused")
		}},
		httpapi.ReadinessCheck{Name: "cache", Check: func(context.Context) error {
			laterCheckRan = true

			return nil
		}},
	))

	status, body := request(t, handler, http.MethodGet, "/readyz")

	if want := http.StatusServiceUnavailable; status != want {
		t.Fatalf("status = %d, want %d", status, want)
	}

	if want := "not ready"; body != want {
		t.Errorf("body = %q, want %q", body, want)
	}

	for _, leak := range []string{"primary-db", "dial tcp: connection refused"} {
		if strings.Contains(body, leak) {
			t.Errorf("body = %q, want it to not disclose %q", body, leak)
		}
	}

	if !laterCheckRan {
		t.Error("check after the failing one did not run, want every check to run")
	}
}

func TestHealthzStaysOKWhileNotReady(t *testing.T) {
	t.Parallel()

	handler := httpapi.NewHandler(httpapi.HealthRoutes(
		httpapi.ReadinessCheck{Name: "primary-db", Check: func(context.Context) error {
			return errors.New("dial tcp: connection refused")
		}},
	))

	if status, _ := request(t, handler, http.MethodGet, "/readyz"); status != http.StatusServiceUnavailable {
		t.Fatalf("readyz status = %d, want %d", status, http.StatusServiceUnavailable)
	}

	status, body := request(t, handler, http.MethodGet, "/healthz")

	if want := http.StatusOK; status != want {
		t.Fatalf("healthz status = %d, want %d", status, want)
	}

	if want := "ok"; body != want {
		t.Errorf("healthz body = %q, want %q", body, want)
	}
}

func TestReadyzRejectsNonGet(t *testing.T) {
	t.Parallel()

	status, _ := request(t, httpapi.NewHandler(httpapi.HealthRoutes()), http.MethodPost, "/readyz")

	if want := http.StatusMethodNotAllowed; status != want {
		t.Fatalf("status = %d, want %d", status, want)
	}
}

func TestReadinessCheckReceivesRequestContext(t *testing.T) {
	t.Parallel()

	type ctxKey struct{}

	handler := httpapi.NewHandler(httpapi.HealthRoutes(
		httpapi.ReadinessCheck{Name: "context", Check: func(ctx context.Context) error {
			if ctx.Value(ctxKey{}) != "value" {
				return errors.New("request context was not passed to the check")
			}

			return nil
		}},
	))

	ctx := context.WithValue(t.Context(), ctxKey{}, "value")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, "/readyz", nil))

	res := rec.Result()
	defer res.Body.Close()

	if got, want := res.StatusCode, http.StatusOK; got != want {
		t.Fatalf("status = %d, want %d", got, want)
	}
}

func TestReadinessCheckHasDeadline(t *testing.T) {
	t.Parallel()

	hasDeadline := false
	handler := httpapi.NewHandler(httpapi.HealthRoutes(
		httpapi.ReadinessCheck{Name: "deadline", Check: func(ctx context.Context) error {
			_, hasDeadline = ctx.Deadline()

			return nil
		}},
	))

	if status, _ := request(t, handler, http.MethodGet, "/readyz"); status != http.StatusOK {
		t.Fatalf("status = %d, want %d", status, http.StatusOK)
	}

	if !hasDeadline {
		t.Error("readiness check context has no deadline, want one")
	}
}
