// Package dashboard implements the lightweight HTTP inspection UI mounted
// by cmd/worker: a workflow list at "/" and a per-workflow detail (status
// plus full event log) at "/workflows/{id}".
package dashboard

import (
	"encoding/json"
	"html/template"
	"net/http"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/srikarjy/workflow_engine/internal/store"
)

var listTmpl = template.Must(template.New("list").Parse(`<!doctype html>
<html><head><title>Workflows</title><style>
body{font-family:monospace;margin:2rem;background:#111;color:#eee}
table{border-collapse:collapse;width:100%}
th,td{text-align:left;padding:.4rem .8rem;border-bottom:1px solid #333}
th{color:#888}
a{color:#6cf}
.status-completed{color:#6f6}
.status-failed{color:#f66}
.status-compensated{color:#fa6}
.status-running{color:#ff6}
</style></head><body>
<h1>Workflows</h1>
<table>
<tr><th>ID</th><th>Name</th><th>Status</th><th>Created</th></tr>
{{range .}}<tr>
<td><a href="/workflows/{{.ID}}">{{.ID}}</a></td>
<td>{{.Name}}</td>
<td class="status-{{.Status}}">{{.Status}}</td>
<td>{{.CreatedAt}}</td>
</tr>{{end}}
</table>
</body></html>`))

var detailTmpl = template.Must(template.New("detail").Parse(`<!doctype html>
<html><head><title>Workflow {{.Workflow.ID}}</title><style>
body{font-family:monospace;margin:2rem;background:#111;color:#eee}
table{border-collapse:collapse;width:100%}
th,td{text-align:left;padding:.4rem .8rem;border-bottom:1px solid #333;vertical-align:top}
th{color:#888}
a{color:#6cf}
pre{white-space:pre-wrap;margin:0}
</style></head><body>
<p><a href="/">&larr; all workflows</a></p>
<h1>{{.Workflow.Name}} <small>{{.Workflow.Status}}</small></h1>
<p>ID: {{.Workflow.ID}}<br>Created: {{.Workflow.CreatedAt}}<br>Updated: {{.Workflow.UpdatedAt}}</p>
<h2>Event log</h2>
<table>
<tr><th>#</th><th>Step</th><th>Type</th><th>Payload</th><th>At</th></tr>
{{range .Events}}<tr>
<td>{{.ID}}</td><td>{{.StepName}}</td><td>{{.Type}}</td>
<td><pre>{{printf "%s" .Payload}}</pre></td><td>{{.CreatedAt}}</td>
</tr>{{end}}
</table>
</body></html>`))

// Config controls the dashboard's exposure: auth and request throughput.
// Zero values mean "no protection" (the default for local development —
// go run ./cmd/worker with no flags behaves exactly as before this existed).
// Any public deployment should set AuthToken at minimum: without it,
// workflow payloads (arbitrary step input/output — e.g. customer data in
// the example order saga) are readable by anyone with the URL.
type Config struct {
	AuthToken      string  // required "Authorization: Bearer <token>" value; "" disables auth
	RateLimitRPS   float64 // sustained requests/sec across all clients; <=0 disables rate limiting
	RateLimitBurst int     // burst allowance above RateLimitRPS
}

// Handler returns a ServeMux serving the dashboard (workflow list and
// detail views) plus Prometheus metrics at /metrics, against s, wrapped in
// the auth and rate-limit middleware described by cfg.
func Handler(s *store.Store, cfg Config) http.Handler {
	mux := http.NewServeMux()

	mux.Handle("GET /metrics", promhttp.Handler())

	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		workflows, err := s.ListWorkflows(r.Context(), 100)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = listTmpl.Execute(w, workflows)
	})

	mux.HandleFunc("GET /workflows/{id}", func(w http.ResponseWriter, r *http.Request) {
		wfID, err := uuid.Parse(r.PathValue("id"))
		if err != nil {
			http.Error(w, "invalid workflow id", http.StatusBadRequest)
			return
		}

		wf, err := s.GetWorkflow(r.Context(), wfID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		events, err := s.ReplayEvents(r.Context(), wfID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = detailTmpl.Execute(w, struct {
			Workflow store.Workflow
			Events   []store.Event
		}{wf, events})
	})

	mux.HandleFunc("GET /workflows/{id}/json", func(w http.ResponseWriter, r *http.Request) {
		wfID, err := uuid.Parse(r.PathValue("id"))
		if err != nil {
			http.Error(w, "invalid workflow id", http.StatusBadRequest)
			return
		}
		wf, err := s.GetWorkflow(r.Context(), wfID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		events, err := s.ReplayEvents(r.Context(), wfID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"workflow": wf, "events": events})
	})

	protected := rateLimitMiddleware(cfg.RateLimitRPS, cfg.RateLimitBurst, authMiddleware(cfg.AuthToken, mux))

	top := http.NewServeMux()
	// Deliberately outside the auth/rate-limit wrapping: a platform health
	// check (e.g. Fly.io) has no way to carry the bearer token, and this
	// exposes nothing sensitive (a static 200, no workflow data).
	top.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	top.Handle("/", protected)
	return top
}
