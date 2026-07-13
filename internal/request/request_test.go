package request

import "testing"

func TestStateTransitions(t *testing.T) {
	valid := [][2]State{{Received, Validated}, {Validated, Routing}, {Routing, InProgress}, {InProgress, Completed}}
	for _, pair := range valid {
		if err := Transition(pair[0], pair[1]); err != nil {
			t.Errorf("Transition(%s,%s) error = %v", pair[0], pair[1], err)
		}
	}
	if err := Transition(Completed, InProgress); err == nil {
		t.Fatal("terminal state transitioned")
	}
}

func TestChatInputValidation(t *testing.T) {
	valid := ChatInput{Model: "aegis-small", Messages: []Message{{Role: "user", Content: "hello"}}}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	invalid := valid
	invalid.Messages = []Message{{Role: "root", Content: "hello"}}
	if err := invalid.Validate(); err == nil {
		t.Fatal("unsupported role accepted")
	}
}
