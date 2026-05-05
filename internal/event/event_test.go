package event_test

import (
	"testing"

	"github.com/InsomniaCoder/claude-go-poc/internal/event"
)

func TestValidate_MissingActorID(t *testing.T) {
	r := event.CreateRequest{Action: "post.liked"}
	if err := r.Validate(); err == nil {
		t.Fatal("expected error for missing actor_id")
	}
}

func TestValidate_MissingAction(t *testing.T) {
	r := event.CreateRequest{ActorID: "u1"}
	if err := r.Validate(); err == nil {
		t.Fatal("expected error for missing action")
	}
}

func TestValidate_Valid(t *testing.T) {
	r := event.CreateRequest{ActorID: "u1", Action: "post.liked"}
	if err := r.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNew_SetsFields(t *testing.T) {
	r := event.CreateRequest{ActorID: "u1", Action: "post.liked", TargetID: "p42"}
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
	if e.CreatedAt.IsZero() {
		t.Error("CreatedAt should not be zero")
	}
}

func TestNew_UniqueIDs(t *testing.T) {
	r := event.CreateRequest{ActorID: "u1", Action: "post.liked"}
	e1 := event.New(r)
	e2 := event.New(r)
	if e1.ID == e2.ID {
		t.Error("expected unique IDs for each New call")
	}
}
