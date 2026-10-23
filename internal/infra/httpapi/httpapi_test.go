package httpapi_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pj-hoakari/jetstream-http-push/internal/infra/httpapi"
)

func request(t *testing.T, handler http.Handler, method, path string) (int, string) {
	t.Helper()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(method, path, nil))

	res := rec.Result()
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	return res.StatusCode, string(body)
}

func TestNewHandlerAppliesEveryRoutes(t *testing.T) {
	t.Parallel()

	handler := httpapi.NewHandler(
		httpapi.HealthRoutes(),
		func(mux *http.ServeMux) {
			mux.HandleFunc("GET /first", func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			})
		},
		func(mux *http.ServeMux) {
			mux.HandleFunc("GET /second", func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			})
		},
	)

	for _, path := range []string{"/healthz", "/readyz"} {
		if got, want := statusOf(t, handler, path), http.StatusOK; got != want {
			t.Errorf("GET %s status = %d, want %d", path, got, want)
		}
	}

	for _, path := range []string{"/first", "/second"} {
		if got, want := statusOf(t, handler, path), http.StatusNoContent; got != want {
			t.Errorf("GET %s status = %d, want %d", path, got, want)
		}
	}
}

func TestUnknownPathIsNotFound(t *testing.T) {
	t.Parallel()

	handler := httpapi.NewHandler(httpapi.HealthRoutes())

	got, _ := request(t, handler, http.MethodPost, "/unknown")
	if want := http.StatusNotFound; got != want {
		t.Fatalf("status = %d, want %d", got, want)
	}
}

func statusOf(t *testing.T, handler http.Handler, path string) int {
	t.Helper()

	status, _ := request(t, handler, http.MethodGet, path)

	return status
}
