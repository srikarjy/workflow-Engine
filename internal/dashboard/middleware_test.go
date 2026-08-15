package dashboard

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

func TestAuthMiddleware_EmptyTokenDisablesCheck(t *testing.T) {
	h := authMiddleware("", okHandler())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 (no token configured -> auth disabled)", rec.Code)
	}
}

func TestAuthMiddleware_RejectsMissingOrWrongToken(t *testing.T) {
	h := authMiddleware("secret", okHandler())

	cases := []struct {
		name   string
		header string
	}{
		{"missing header", ""},
		{"wrong token", "Bearer wrong"},
		{"missing Bearer prefix", "secret"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if c.header != "" {
				req.Header.Set("Authorization", c.header)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want 401", rec.Code)
			}
		})
	}
}

func TestAuthMiddleware_AcceptsCorrectToken(t *testing.T) {
	h := authMiddleware("secret", okHandler())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}

func TestRateLimitMiddleware_NonPositiveRPSDisablesLimiting(t *testing.T) {
	h := rateLimitMiddleware(0, 1, okHandler())
	for i := 0; i < 20; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200 (rate limiting disabled)", i, rec.Code)
		}
	}
}

func TestRateLimitMiddleware_RejectsAboveBurst(t *testing.T) {
	h := rateLimitMiddleware(1, 3, okHandler())

	var ok, limited int
	for i := 0; i < 6; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		switch rec.Code {
		case http.StatusOK:
			ok++
		case http.StatusTooManyRequests:
			limited++
		default:
			t.Errorf("request %d: unexpected status %d", i, rec.Code)
		}
	}
	if ok != 3 {
		t.Errorf("allowed requests = %d, want 3 (the burst)", ok)
	}
	if limited != 3 {
		t.Errorf("rate-limited requests = %d, want 3", limited)
	}
}
