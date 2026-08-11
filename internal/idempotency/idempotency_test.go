package idempotency

import "testing"

func TestDedupKey_DeterministicForSameInputs(t *testing.T) {
	input := map[string]any{"amount": 100, "currency": "USD"}
	k1, err := DedupKey("wf-1", "charge_payment", input)
	if err != nil {
		t.Fatal(err)
	}
	k2, err := DedupKey("wf-1", "charge_payment", input)
	if err != nil {
		t.Fatal(err)
	}
	if k1 != k2 {
		t.Errorf("same inputs produced different keys: %q vs %q", k1, k2)
	}
}

func TestDedupKey_DiffersByWorkflowID(t *testing.T) {
	input := map[string]any{"amount": 100}
	k1, _ := DedupKey("wf-1", "charge_payment", input)
	k2, _ := DedupKey("wf-2", "charge_payment", input)
	if k1 == k2 {
		t.Error("different workflow IDs produced the same dedup key")
	}
}

func TestDedupKey_DiffersByStepName(t *testing.T) {
	input := map[string]any{"amount": 100}
	k1, _ := DedupKey("wf-1", "charge_payment", input)
	k2, _ := DedupKey("wf-1", "refund_payment", input)
	if k1 == k2 {
		t.Error("different step names produced the same dedup key")
	}
}

func TestDedupKey_DiffersByInput(t *testing.T) {
	k1, _ := DedupKey("wf-1", "charge_payment", map[string]any{"amount": 100})
	k2, _ := DedupKey("wf-1", "charge_payment", map[string]any{"amount": 200})
	if k1 == k2 {
		t.Error("different inputs produced the same dedup key")
	}
}

func TestDedupKey_HandlesNilInput(t *testing.T) {
	k1, err := DedupKey("wf-1", "step", nil)
	if err != nil {
		t.Fatal(err)
	}
	k2, err := DedupKey("wf-1", "step", nil)
	if err != nil {
		t.Fatal(err)
	}
	if k1 != k2 {
		t.Error("nil input should still be deterministic")
	}
}

func TestDedupKey_IsHexSHA256(t *testing.T) {
	k, err := DedupKey("wf-1", "step", map[string]any{"a": 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(k) != 64 {
		t.Errorf("key length = %d, want 64 (hex-encoded SHA-256)", len(k))
	}
	for _, c := range k {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Fatalf("key contains non-hex character %q: %s", c, k)
		}
	}
}

// TestDedupKey_MapKeyOrderDoesNotAffectResult documents a property the
// engine relies on: two logically-identical inputs built by iterating a Go
// map (whose iteration order is randomized) must hash to the same key.
// encoding/json already sorts map[string]any keys when marshaling, so
// DedupKey is order-independent for map inputs without any extra work.
func TestDedupKey_MapKeyOrderDoesNotAffectResult(t *testing.T) {
	a := map[string]any{"zebra": 1, "apple": 2, "mango": 3}
	b := map[string]any{"mango": 3, "zebra": 1, "apple": 2}

	ka, err := DedupKey("wf-1", "step", a)
	if err != nil {
		t.Fatal(err)
	}
	kb, err := DedupKey("wf-1", "step", b)
	if err != nil {
		t.Fatal(err)
	}
	if ka != kb {
		t.Error("map key insertion order affected the dedup key; encoding/json should have made this order-independent")
	}
}

// TestDedupKeyFromMap_MatchesDedupKey documents that DedupKeyFromMap's
// manual key-sorting is redundant with what encoding/json already does for
// map[string]any — see TestDedupKey_MapKeyOrderDoesNotAffectResult. It
// should always agree with plain DedupKey given the same map.
func TestDedupKeyFromMap_MatchesDedupKey(t *testing.T) {
	input := map[string]any{"b": 2, "a": 1, "c": 3}

	want, err := DedupKey("wf-1", "step", input)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DedupKeyFromMap("wf-1", "step", input)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("DedupKeyFromMap = %q, want it to match DedupKey = %q", got, want)
	}
}

func TestDedupKeyFromMap_HandlesNilInput(t *testing.T) {
	k1, err := DedupKeyFromMap("wf-1", "step", nil)
	if err != nil {
		t.Fatal(err)
	}
	k2, err := DedupKey("wf-1", "step", nil)
	if err != nil {
		t.Fatal(err)
	}
	if k1 != k2 {
		t.Error("DedupKeyFromMap(nil) should match DedupKey(nil)")
	}
}
