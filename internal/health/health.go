// Package health provides Kubernetes-compatible liveness and readiness handlers.
package health

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
)

// State owns the readiness state for one process.
type State struct {
	ready atomic.Bool
}

// NewState creates a process that starts unready.
func NewState() *State { return &State{} }

// SetReady atomically changes whether new work may be accepted.
func (s *State) SetReady(ready bool) { s.ready.Store(ready) }

// IsReady returns the current readiness state.
func (s *State) IsReady() bool { return s.ready.Load() }

// Live reports process liveness. Dependency failures must not cause restarts here.
func (s *State) Live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
}

// Ready reports whether the service can currently accept new work.
func (s *State) Ready(w http.ResponseWriter, _ *http.Request) {
	if !s.ready.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
