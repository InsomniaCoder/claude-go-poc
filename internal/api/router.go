// internal/api/router.go
package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func NewRouter(h *Handler) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Get("/healthz", h.Health)
	r.Post("/events", h.PublishEvent)
	r.Get("/timeline", h.Timeline)
	return r
}
