package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/InsomniaCoder/claude-go-poc/internal/api"
	"github.com/InsomniaCoder/claude-go-poc/internal/event"
	"github.com/InsomniaCoder/claude-go-poc/internal/store"
)

type mockStore struct {
	insertFn   func(context.Context, event.Event) error
	timelineFn func(context.Context, store.TimelineQuery) ([]event.Event, error)
}

func (m *mockStore) Insert(ctx context.Context, e event.Event) error {
	return m.insertFn(ctx, e)
}
func (m *mockStore) Timeline(ctx context.Context, q store.TimelineQuery) ([]event.Event, error) {
	return m.timelineFn(ctx, q)
}
func (m *mockStore) Close() error { return nil }

type mockProducer struct {
	publishFn func(context.Context, string, any) error
}

func (m *mockProducer) Publish(ctx context.Context, topic string, value any) error {
	if m.publishFn != nil {
		return m.publishFn(ctx, topic, value)
	}
	return nil
}
func (m *mockProducer) Close() {}

func TestHealth(t *testing.T) {
	h := api.NewHandler(&mockStore{}, &mockProducer{}, "t")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	h.Health(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestPublishEvent_MissingActorID(t *testing.T) {
	h := api.NewHandler(&mockStore{}, &mockProducer{}, "t")
	body, _ := json.Marshal(event.CreateRequest{Action: "post.liked"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
	h.PublishEvent(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestPublishEvent_MissingAction(t *testing.T) {
	h := api.NewHandler(&mockStore{}, &mockProducer{}, "t")
	body, _ := json.Marshal(event.CreateRequest{ActorID: "u1"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
	h.PublishEvent(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestPublishEvent_ProducerError_Returns503(t *testing.T) {
	h := api.NewHandler(
		&mockStore{},
		&mockProducer{publishFn: func(_ context.Context, _ string, _ any) error {
			return errors.New("broker unavailable")
		}},
		"t",
	)
	body, _ := json.Marshal(event.CreateRequest{ActorID: "u1", Action: "post.liked"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
	h.PublishEvent(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestPublishEvent_Success(t *testing.T) {
	var publishedTopic string
	h := api.NewHandler(
		&mockStore{},
		&mockProducer{publishFn: func(_ context.Context, topic string, _ any) error {
			publishedTopic = topic
			return nil
		}},
		"activity-events",
	)
	body, _ := json.Marshal(event.CreateRequest{ActorID: "u1", Action: "post.liked", TargetID: "p42"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
	h.PublishEvent(rec, req)
	if rec.Code != http.StatusCreated {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusCreated)
	}
	if publishedTopic != "activity-events" {
		t.Errorf("topic = %q, want %q", publishedTopic, "activity-events")
	}
	var e event.Event
	if err := json.NewDecoder(rec.Body).Decode(&e); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if e.ID == "" {
		t.Error("response event ID should not be empty")
	}
	if e.ActorID != "u1" {
		t.Errorf("ActorID = %q, want %q", e.ActorID, "u1")
	}
}

func TestTimeline_ReturnsEvents(t *testing.T) {
	want := []event.Event{{ID: "abc", ActorID: "u1", Action: "post.liked", CreatedAt: time.Now()}}
	h := api.NewHandler(
		&mockStore{timelineFn: func(_ context.Context, q store.TimelineQuery) ([]event.Event, error) {
			if q.ActorID != "u1" {
				t.Errorf("ActorID filter = %q, want %q", q.ActorID, "u1")
			}
			return want, nil
		}},
		&mockProducer{},
		"t",
	)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/timeline?actor_id=u1", nil)
	h.Timeline(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var got []event.Event
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 1 || got[0].ID != "abc" {
		t.Errorf("events = %v, want 1 event with ID=abc", got)
	}
}

func TestTimeline_EmptyReturnsEmptyArray(t *testing.T) {
	h := api.NewHandler(
		&mockStore{timelineFn: func(_ context.Context, _ store.TimelineQuery) ([]event.Event, error) {
			return nil, nil
		}},
		&mockProducer{},
		"t",
	)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/timeline", nil)
	h.Timeline(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	if body != "[]\n" {
		t.Errorf("body = %q, want %q", body, "[]\n")
	}
}
