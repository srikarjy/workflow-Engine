package dashboard

import (
	"crypto/subtle"
	"net/http"

	"golang.org/x/time/rate"
)

// authMiddleware requires a "Authorization: Bearer <token>" header matching
// token on every request. An empty token disables the check entirely — the
// default for local development (`go run ./cmd/worker` with no -auth-token
// set behaves exactly as before). Any public deployment must set one:
// leaving the dashboard unauthenticated exposes workflow payloads (arbitrary
// step input/output, e.g. customer data) to the whole internet.
func authMiddleware(token string, next http.Handler) http.Handler {
	if token == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const prefix = "Bearer "
		got := r.Header.Get("Authorization")
		if len(got) != len(prefix)+len(token) ||
			subtle.ConstantTimeCompare([]byte(got), []byte(prefix+token)) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="workflow-dashboard"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// rateLimitMiddleware caps total request throughput across all clients with
// a single shared token bucket (rps sustained, burst allowed above that).
// This is deliberately global rather than per-IP: the goal is bounding the
// worst case cost of a public, single-node demo deployment (compute/DB load
// from someone hammering it), not per-client fairness — a per-IP limiter
// would need unbounded map growth handling for what is a portfolio safety
// net, not a production API gateway. rps <= 0 disables it.
func rateLimitMiddleware(rps float64, burst int, next http.Handler) http.Handler {
	if rps <= 0 {
		return next
	}
	limiter := rate.NewLimiter(rate.Limit(rps), burst)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !limiter.Allow() {
			http.Error(w, "rate limit exceeded, try again shortly", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}
