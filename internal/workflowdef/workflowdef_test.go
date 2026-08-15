package workflowdef

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/srikarjy/workflow_engine/internal/httpstep"
)

const fixture = `
name: order-saga
retry:
  max_attempts: 3
  initial_backoff: 100ms
  max_backoff: 2s
  jitter: true
steps:
  - name: reserve_inventory
    execute:
      method: POST
      url: http://localhost:9090/reserve
      timeout: 5s
    compensate:
      method: POST
      url: http://localhost:9090/release
  - name: charge_payment
    execute: {method: POST, url: http://localhost:9090/charge}
    compensate: {method: POST, url: http://localhost:9090/refund}
    retry: {max_attempts: 5}
  - name: update_analytics
    execute: {method: POST, url: http://localhost:9090/analytics}
`

func writeFixture(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "workflow.yaml")
	if err := os.WriteFile(path, []byte(fixture), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoad(t *testing.T) {
	def, err := Load(writeFixture(t))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if def.Name != "order-saga" {
		t.Errorf("Name = %q, want order-saga", def.Name)
	}
	if len(def.Steps) != 3 {
		t.Fatalf("len(Steps) = %d, want 3", len(def.Steps))
	}
	if def.Steps[0].Compensate == nil || def.Steps[0].Compensate.URL != "http://localhost:9090/release" {
		t.Errorf("step 0 compensate = %+v, want release URL", def.Steps[0].Compensate)
	}
	if def.Steps[2].Compensate != nil {
		t.Errorf("step 2 (update_analytics) compensate = %+v, want nil", def.Steps[2].Compensate)
	}
	if def.Retry.InitialBackoff != 100*time.Millisecond {
		t.Errorf("Retry.InitialBackoff = %v, want 100ms", def.Retry.InitialBackoff)
	}
}

func TestLoad_MissingFieldsRejected(t *testing.T) {
	cases := map[string]string{
		"no name":          "steps:\n  - name: a\n    execute: {url: http://x}\n",
		"no steps":         "name: x\n",
		"step missing url": "name: x\nsteps:\n  - name: a\n    execute: {method: POST}\n",
	}
	for label, content := range cases {
		t.Run(label, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "workflow.yaml")
			if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(path); err == nil {
				t.Error("Load: want error, got nil")
			}
		})
	}
}

func TestBuild_ResolvesRetryPolicy(t *testing.T) {
	def, err := Load(writeFixture(t))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	wfDef := def.Build()
	if wfDef.Name != "order-saga" {
		t.Errorf("Name = %q", wfDef.Name)
	}
	if len(wfDef.Steps) != 3 {
		t.Fatalf("len(Steps) = %d, want 3", len(wfDef.Steps))
	}
	for i, name := range []string{"reserve_inventory", "charge_payment", "update_analytics"} {
		if wfDef.Steps[i].Name() != name {
			t.Errorf("Steps[%d].Name() = %q, want %q", i, wfDef.Steps[i].Name(), name)
		}
	}

	// step 0 has no retry override -> inherits workflow default (max_attempts: 3)
	// step 1 overrides to max_attempts: 5
	// step 2 has neither -> also inherits workflow default
	// Exercised indirectly: resolveRetry is unit-testable directly since it's unexported.
	got := resolveRetry(def.Steps[1].Retry, def.Retry)
	if got.MaxAttempts != 5 {
		t.Errorf("step 1 resolved MaxAttempts = %d, want 5 (override)", got.MaxAttempts)
	}
	got = resolveRetry(def.Steps[0].Retry, def.Retry)
	if got.MaxAttempts != 3 {
		t.Errorf("step 0 resolved MaxAttempts = %d, want 3 (workflow default)", got.MaxAttempts)
	}
	got = resolveRetry(nil, nil)
	if got != httpstep.DefaultRetryPolicy {
		t.Errorf("resolveRetry(nil, nil) = %+v, want httpstep.DefaultRetryPolicy", got)
	}
}
