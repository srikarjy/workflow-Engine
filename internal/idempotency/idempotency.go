// Package idempotency derives deterministic deduplication keys for workflow steps.
package idempotency

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// DedupKey generates a deterministic SHA-256 deduplication key from a workflow
// ID, step name, and step input. The same inputs will always produce the same
// key, enabling exactly-once execution by detecting duplicate completion attempts.
func DedupKey(workflowID, stepName string, input any) (string, error) {
	var inputBytes []byte
	var err error

	if input != nil {
		inputBytes, err = json.Marshal(input)
		if err != nil {
			return "", fmt.Errorf("idempotency: marshal input: %w", err)
		}
	} else {
		inputBytes = []byte("null")
	}

	// Create a deterministic string representation
	// Format: workflowID|stepName|inputJSON
	// Using a delimiter that won't appear in the JSON
	keyString := workflowID + "|" + stepName + "|" + string(inputBytes)

	hash := sha256.Sum256([]byte(keyString))
	return hex.EncodeToString(hash[:]), nil
}

// DedupKeyFromMap generates a deduplication key from a map, ensuring
// deterministic key ordering for consistent hashing.
func DedupKeyFromMap(workflowID, stepName string, input map[string]any) (string, error) {
	if input == nil {
		return DedupKey(workflowID, stepName, nil)
	}

	// Sort keys for deterministic ordering
	keys := make([]string, 0, len(input))
	for k := range input {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build ordered map for consistent JSON marshaling
	ordered := make(map[string]any, len(input))
	for _, k := range keys {
		ordered[k] = input[k]
	}

	return DedupKey(workflowID, stepName, ordered)
}