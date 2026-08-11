package steps

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"github.com/srikarjy/workflow_engine/internal/engine"
)

// metalswHit is one line of gpu_main's stdout: "%-20s %d\n" (id, score).
type metalswHit struct {
	ID    string `json:"id"`
	Score int    `json:"score"`
}

var mismatchLineRe = regexp.MustCompile(`^(\d+)/(\d+) mismatches vs CPU oracle$`)

// metalswStep runs github.com/srikarjy/metalsw's gpu_main binary as a
// subprocess: `gpu_main <query.fasta> <db.fasta> [topN] [metallib_path]`.
// Results (top-N hits) are on stdout as "%-20s %d\n" per hit; per-record
// GPU-vs-CPU-oracle diagnostics and a final "N/M mismatches vs CPU oracle"
// summary are on stderr. gpu_main exits non-zero if any oracle mismatch
// occurred — that's a correctness signal, not just a process failure, so
// Execute treats it as an error rather than returning partial results
// silently.
//
// KNOWN LIMITATION: hit-line parsing assumes FASTA IDs contain no
// whitespace (splits each stdout line on whitespace). An ID with an
// embedded space would parse incorrectly. Not validated against real
// SwissProt-style headers with descriptions — only the bundled
// data/*.fasta test fixtures.
type metalswStep struct {
	binPath      string
	metallibPath string
}

// NewMetalSWStep returns a StepExecutor that runs the compiled gpu_main
// binary at binPath, using the Metal shader library at metallibPath.
func NewMetalSWStep(binPath, metallibPath string) engine.StepExecutor {
	return &metalswStep{binPath: binPath, metallibPath: metallibPath}
}

func (s *metalswStep) Name() string { return "metalsw" }

func (s *metalswStep) Execute(ctx context.Context, input map[string]any) (map[string]any, error) {
	queryPath, ok := input["query_fasta"].(string)
	if !ok || queryPath == "" {
		return nil, fmt.Errorf("metalsw: missing required input \"query_fasta\"")
	}
	dbPath, ok := input["db_fasta"].(string)
	if !ok || dbPath == "" {
		return nil, fmt.Errorf("metalsw: missing required input \"db_fasta\"")
	}
	topN := 10
	switch v := input["top_n"].(type) {
	case float64:
		topN = int(v)
	case int:
		topN = v
	}

	args := []string{queryPath, dbPath, strconv.Itoa(topN)}
	if s.metallibPath != "" {
		args = append(args, s.metallibPath)
	}

	cmd := exec.CommandContext(ctx, s.binPath, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	runErr := cmd.Run()

	hits, parseErr := parseMetalSWHits(stdout.String())
	if parseErr != nil {
		return nil, fmt.Errorf("metalsw: parse stdout: %w", parseErr)
	}

	mismatches, mismatchTotal := parseMismatchSummary(stderr.String())

	if runErr != nil {
		// A non-zero exit with parsed mismatch counts means gpu_main ran
		// but disagreed with the CPU oracle on some records -- surface that
		// specifically rather than a bare exit-status error.
		if mismatchTotal > 0 {
			return nil, fmt.Errorf("metalsw: %d/%d records mismatched CPU oracle: %w", mismatches, mismatchTotal, runErr)
		}
		return nil, fmt.Errorf("metalsw: gpu_main failed: %w (stderr: %s)", runErr, strings.TrimSpace(stderr.String()))
	}

	return map[string]any{
		"hits":       hits,
		"mismatches": mismatches,
		"total":      mismatchTotal,
	}, nil
}

// Compensate is a no-op: a search has no side effect to undo.
func (s *metalswStep) Compensate(ctx context.Context, output map[string]any) error {
	return nil
}

func parseMetalSWHits(stdout string) ([]metalswHit, error) {
	var hits []metalswHit
	scanner := bufio.NewScanner(strings.NewReader(stdout))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return nil, fmt.Errorf("unexpected line format: %q", line)
		}
		score, err := strconv.Atoi(fields[len(fields)-1])
		if err != nil {
			return nil, fmt.Errorf("parse score in line %q: %w", line, err)
		}
		id := strings.Join(fields[:len(fields)-1], " ")
		hits = append(hits, metalswHit{ID: id, Score: score})
	}
	return hits, scanner.Err()
}

// parseMismatchSummary looks for gpu_main's stderr summary line,
// "<mismatches>/<total> mismatches vs CPU oracle". Returns 0, 0 if not found
// (e.g. gpu_main failed before reaching that point).
func parseMismatchSummary(stderr string) (mismatches, total int) {
	scanner := bufio.NewScanner(strings.NewReader(stderr))
	for scanner.Scan() {
		m := mismatchLineRe.FindStringSubmatch(strings.TrimSpace(scanner.Text()))
		if m == nil {
			continue
		}
		mismatches, _ = strconv.Atoi(m[1])
		total, _ = strconv.Atoi(m[2])
		return mismatches, total
	}
	return 0, 0
}
