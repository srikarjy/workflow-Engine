package steps

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// fakeGPUMain writes a shell script standing in for gpu_main: it ignores
// its args and prints the given stdout/stderr, exiting with code.
func fakeGPUMain(t *testing.T, stdout, stderr string, code int) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "gpu_main")
	script := "#!/bin/sh\n" +
		"cat <<'STDOUT_EOF'\n" + stdout + "STDOUT_EOF\n" +
		"cat <<'STDERR_EOF' >&2\n" + stderr + "STDERR_EOF\n" +
		"exit " + strconv.Itoa(code) + "\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestMetalSWStep_Success(t *testing.T) {
	bin := fakeGPUMain(t,
		"sp|P69905|HBA_HUMAN     142\nsp|P68871|HBB_HUMAN     138\n",
		"sp|P69905|HBA_HUMAN gpu=142    oracle=142    PASS\nsp|P68871|HBB_HUMAN gpu=138    oracle=138    PASS\n\n2 sequences, 0.01s, 1.0 GCUPS\n0/2 mismatches vs CPU oracle\n",
		0,
	)
	step := NewMetalSWStep(bin, "")

	out, err := step.Execute(context.Background(), map[string]any{
		"query_fasta": "query.fasta",
		"db_fasta":    "db.fasta",
		"top_n":       float64(10),
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	hits, ok := out["hits"].([]metalswHit)
	if !ok || len(hits) != 2 {
		t.Fatalf("expected 2 hits, got %#v", out["hits"])
	}
	if hits[0].ID != "sp|P69905|HBA_HUMAN" || hits[0].Score != 142 {
		t.Errorf("hit 0 = %+v, want id=sp|P69905|HBA_HUMAN score=142", hits[0])
	}
	if out["mismatches"] != 0 || out["total"] != 2 {
		t.Errorf("mismatches/total = %v/%v, want 0/2", out["mismatches"], out["total"])
	}
}

func TestMetalSWStep_OracleMismatch(t *testing.T) {
	bin := fakeGPUMain(t,
		"sp|BAD|SEQ     99\n",
		"sp|BAD|SEQ gpu=99 oracle=105 FAIL\n\n1 sequences, 0.01s, 1.0 GCUPS\n1/1 mismatches vs CPU oracle\n",
		1,
	)
	step := NewMetalSWStep(bin, "")

	_, err := step.Execute(context.Background(), map[string]any{
		"query_fasta": "query.fasta",
		"db_fasta":    "db.fasta",
	})
	if err == nil {
		t.Fatal("expected an error on oracle mismatch, got nil")
	}
}

func TestMetalSWStep_MissingInput(t *testing.T) {
	step := NewMetalSWStep("/bin/true", "")
	_, err := step.Execute(context.Background(), map[string]any{"db_fasta": "db.fasta"})
	if err == nil {
		t.Fatal("expected an error for missing query_fasta, got nil")
	}
}
