// Package store benchmarks.
package store

import (
	"testing"
)

func BenchmarkStore_AppendEvent(b *testing.B) {
	b.Skip("Requires PostgreSQL connection")
}

func BenchmarkStore_HasCompleted(b *testing.B) {
	b.Skip("Requires PostgreSQL connection")
}

func BenchmarkStore_ReplayEvents(b *testing.B) {
	b.Skip("Requires PostgreSQL connection")
}

func BenchmarkStore_CreateWorkflow(b *testing.B) {
	b.Skip("Requires PostgreSQL connection")
}