package event_test

import (
	"testing"

	"github.com/InsomniaCoder/claude-go-poc/internal/event"
)

func TestValidate_MissingActorID(t *testing.T) {
	r := event.CreateRequest{Action: "post.liked"}
	err := r.Validate()
	if err == nil {
		t.Fatal("expected error for missing actor_id")
	}
	if err.Error() != "actor_id is required" {
		t.Errorf("error = %q, want %q", err.Error(), "actor_id is required")
	}
}

func TestValidate_MissingAction(t *testing.T) {
	r := event.CreateRequest{ActorID: "u1"}
	err := r.Validate()
	if err == nil {
		t.Fatal("expected error for missing action")
	}
	if err.Error() != "action is required" {
		t.Errorf("error = %q, want %q", err.Error(), "action is required")
	}
}

func TestValidate_Valid(t *testing.T) {
	r := event.CreateRequest{ActorID: "u1", Action: "post.liked"}
	if err := r.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNew_SetsFields(t *testing.T) {
	r := event.CreateRequest{ActorID: "u1", Action: "post.liked", TargetID: "p42", Payload: `{"key":"val"}`}
	e := event.New(r)
	if e.ID == "" {
		t.Error("ID should not be empty")
	}
	if e.ActorID != "u1" {
		t.Errorf("ActorID = %q, want %q", e.ActorID, "u1")
	}
	if e.Action != "post.liked" {
		t.Errorf("Action = %q, want %q", e.Action, "post.liked")
	}
	if e.TargetID != "p42" {
		t.Errorf("TargetID = %q, want %q", e.TargetID, "p42")
	}
	if e.Payload != `{"key":"val"}` {
		t.Errorf("Payload = %q, want %q", e.Payload, `{"key":"val"}`)
	}
	if e.CreatedAt.IsZero() {
		t.Error("CreatedAt should not be zero")
	}
}

func TestNew_UniqueIDs(t *testing.T) {
	r := event.CreateRequest{ActorID: "u1", Action: "post.liked"}
	ids := map[string]bool{}
	for i := 0; i < 10; i++ {
		e := event.New(r)
		if ids[e.ID] {
			t.Fatalf("duplicate ID generated: %s", e.ID)
		}
		ids[e.ID] = true
	}
}
