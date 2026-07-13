// Package problem writes RFC 9457-style API errors without exposing internal details.
package problem

import (
	"encoding/json"
	"net/http"
)

// Detail is the stable error representation returned by AegisAI APIs.
type Detail struct {
	Type          string `json:"type"`
	Title         string `json:"title"`
	Status        int    `json:"status"`
	Detail        string `json:"detail,omitempty"`
	Instance      string `json:"instance,omitempty"`
	Code          string `json:"code,omitempty"`
	RequestID     string `json:"request_id,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
}

// Write serializes a problem response. Internal error strings must not be passed as Detail.
func Write(w http.ResponseWriter, value Detail) {
	if value.Type == "" {
		value.Type = "about:blank"
	}
	if value.Title == "" {
		value.Title = http.StatusText(value.Status)
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(value.Status)
	_ = json.NewEncoder(w).Encode(value)
}
