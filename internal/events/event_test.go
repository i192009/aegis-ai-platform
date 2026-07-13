package events

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEnvelopeValidation(t *testing.T) {
	event := Envelope{EventID: "e", EventType: RequestCompleted, Version: 1, AggregateID: "r", TenantID: "t", Timestamp: time.Now(), CorrelationID: "c", Payload: json.RawMessage(`{"ok":true}`)}
	if err := event.Validate(); err != nil {
		t.Fatal(err)
	}
	event.Version = 2
	if err := event.Validate(); err == nil {
		t.Fatal("unsupported version accepted")
	}
}
