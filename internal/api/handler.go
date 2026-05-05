package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/InsomniaCoder/claude-go-poc/internal/event"
	"github.com/InsomniaCoder/claude-go-poc/internal/kafka"
	"github.com/InsomniaCoder/claude-go-poc/internal/store"
)

type Handler struct {
	store    store.Store
	producer kafka.Producer
	topic    string
}

func NewHandler(s store.Store, p kafka.Producer, topic string) *Handler {
	return &Handler{store: s, producer: p, topic: topic}
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (h *Handler) PublishEvent(w http.ResponseWriter, r *http.Request) {
	var req event.CreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if err := req.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	e := event.New(req)
	if err := h.producer.Publish(r.Context(), h.topic, e); err != nil {
		slog.Error("publish event", "err", err, "event_id", e.ID)
		http.Error(w, "failed to publish event", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(e)
}

func (h *Handler) Timeline(w http.ResponseWriter, r *http.Request) {
	q := store.TimelineQuery{
		ActorID: r.URL.Query().Get("actor_id"),
		Action:  r.URL.Query().Get("action"),
		Limit:   50,
	}
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil {
			q.Limit = n
		}
	}
	if b := r.URL.Query().Get("before"); b != "" {
		if t, err := time.Parse(time.RFC3339, b); err == nil {
			q.Before = t
		}
	}

	events, err := h.store.Timeline(r.Context(), q)
	if err != nil {
		slog.Error("query timeline", "err", err)
		http.Error(w, "failed to query timeline", http.StatusInternalServerError)
		return
	}
	if events == nil {
		events = []event.Event{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(events)
}
