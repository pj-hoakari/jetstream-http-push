package httpapi

import (
	"net/http"
)

type Routes func(mux *http.ServeMux)

func NewHandler(routes ...Routes) http.Handler {
	mux := http.NewServeMux()

	for _, route := range routes {
		route(mux)
	}

	return mux
}
