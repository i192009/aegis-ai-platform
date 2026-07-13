package health

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestReadiness(t *testing.T) {
	state := NewState()
	recorder := httptest.NewRecorder()
	state.Ready(recorder, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("unready status = %d", recorder.Code)
	}

	state.SetReady(true)
	recorder = httptest.NewRecorder()
	state.Ready(recorder, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("ready status = %d", recorder.Code)
	}
}
