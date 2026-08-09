// Package idempotency benchmarks.
package idempotency

import (
	"testing"
)

func BenchmarkDedupKey(b *testing.B) {
	input := map[string]any{
		"items": []map[string]any{
			{"product_id": "SKU-001", "quantity": 2},
			{"product_id": "SKU-002", "quantity": 1},
		},
	}
	workflowID := "wf-12345678-1234-1234-1234-123456789012"
	stepName := "reserve_inventory"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DedupKey(workflowID, stepName, input)
	}
}

func BenchmarkDedupKey_Simple(b *testing.B) {
	workflowID := "wf-12345678-1234-1234-1234-123456789012"
	stepName := "simple_step"
	input := map[string]any{"value": "test"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DedupKey(workflowID, stepName, input)
	}
}

func BenchmarkDedupKeyFromMap(b *testing.B) {
	workflowID := "wf-12345678-1234-1234-1234-123456789012"
	stepName := "step_with_many_fields"
	input := map[string]any{
		"field1": "value1",
		"field2": "value2",
		"field3": "value3",
		"field4": "value4",
		"field5": "value5",
		"field6": "value6",
		"field7": "value7",
		"field8": "value8",
		"field9": "value9",
		"field10": "value10",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DedupKeyFromMap(workflowID, stepName, input)
	}
}