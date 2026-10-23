package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

const readinessTimeout = 5 * time.Second

type ReadinessCheck struct {
	Name  string
	Check func(ctx context.Context) error
}

func HealthRoutes(checks ...ReadinessCheck) Routes {
	return func(mux *http.ServeMux) {
		mux.HandleFunc("GET /healthz", handleHealthz)
		mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
			handleReadyz(w, r, checks)
		})
	}
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)

	if _, err := w.Write([]byte("ok")); err != nil {
		slog.ErrorContext(r.Context(), "healthz response write failed", "error", err)
	}
}

func handleReadyz(w http.ResponseWriter, r *http.Request, checks []ReadinessCheck) {
	ctx, cancel := context.WithTimeout(r.Context(), readinessTimeout)
	defer cancel()

	status, body := http.StatusOK, "ok"

	for _, check := range checks {
		if err := check.Check(ctx); err != nil {
			slog.WarnContext(ctx, "readiness check failed", "check", check.Name, "error", err)

			status, body = http.StatusServiceUnavailable, "not ready"
		}
	}

	w.WriteHeader(status)

	if _, err := w.Write([]byte(body)); err != nil {
		slog.ErrorContext(ctx, "readyz response write failed", "error", err)
	}
}
