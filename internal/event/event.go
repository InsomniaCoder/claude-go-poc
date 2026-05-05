package event

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

type Event struct {
	ID        string    `json:"id"`
	ActorID   string    `json:"actor_id"`
	Action    string    `json:"action"`
	TargetID  string    `json:"target_id"`
	Payload   string    `json:"payload"`
	CreatedAt time.Time `json:"created_at"`
}

type CreateRequest struct {
	ActorID  string `json:"actor_id"`
	Action   string `json:"action"`
	TargetID string `json:"target_id"`
	Payload  string `json:"payload"`
}

func (r CreateRequest) Validate() error {
	if r.ActorID == "" {
		return errors.New("actor_id is required")
	}
	if r.Action == "" {
		return errors.New("action is required")
	}
	return nil
}

func New(r CreateRequest) Event {
	return Event{
		ID:        uuid.New().String(),
		ActorID:   r.ActorID,
		Action:    r.Action,
		TargetID:  r.TargetID,
		Payload:   r.Payload,
		CreatedAt: time.Now().UTC(),
	}
}
