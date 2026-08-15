package httpstep

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestExecute_ForwardsInputAndParsesOutput(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(data, &gotBody)
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", r.Header.Get("Content-Type"))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "order_id": "abc"})
	}))
	defer srv.Close()

	s := New("reserve_inventory", Call{Method: "POST", URL: srv.URL}, nil, RetryPolicy{MaxAttempts: 1})
	output, err := s.Execute(context.Background(), map[string]any{"sku": "SKU-1"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if gotBody["sku"] != "SKU-1" {
		t.Errorf("server saw input %v, want sku=SKU-1", gotBody)
	}
	if output["status"] != "ok" || output["order_id"] != "abc" {
		t.Errorf("output = %v, want status=ok order_id=abc", output)
	}
}

func TestExecute_RetriesThenSucceeds(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
	}))
	defer srv.Close()

	s := New("flaky", Call{Method: "POST", URL: srv.URL}, nil, RetryPolicy{
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
	})
	output, err := s.Execute(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3 (2 failures + 1 success)", calls)
	}
	if output["status"] != "ok" {
		t.Errorf("output = %v, want status=ok", output)
	}
}

func TestExecute_ExhaustsRetriesAndFails(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	s := New("always_fails", Call{Method: "POST", URL: srv.URL}, nil, RetryPolicy{
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
	})
	_, err := s.Execute(context.Background(), map[string]any{})
	if err == nil {
		t.Fatal("Execute: want error, got nil")
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3 (MaxAttempts)", calls)
	}
}

func TestCompensate_CallsCompensateURL(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(data, &gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	compensate := Call{Method: "POST", URL: srv.URL}
	s := New("reserve_inventory", Call{Method: "POST", URL: srv.URL}, &compensate, RetryPolicy{MaxAttempts: 1})
	if err := s.Compensate(context.Background(), map[string]any{"order_id": "abc"}); err != nil {
		t.Fatalf("Compensate: %v", err)
	}
	if gotBody["order_id"] != "abc" {
		t.Errorf("server saw compensate body %v, want order_id=abc", gotBody)
	}
}

func TestCompensate_NoopWhenNotConfigured(t *testing.T) {
	s := New("update_analytics", Call{Method: "POST", URL: "http://unused"}, nil, RetryPolicy{MaxAttempts: 1})
	if err := s.Compensate(context.Background(), map[string]any{}); err != nil {
		t.Fatalf("Compensate with no compensate call configured: %v", err)
	}
}
