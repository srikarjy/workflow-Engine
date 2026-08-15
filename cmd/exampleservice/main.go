// Command exampleservice is a throwaway HTTP target for examples/order-saga.yaml.
// Every route echoes its JSON request body back with {"status": "ok"}
// merged in. Adding ?fail=N to a route's URL makes that route fail the
// first N requests it receives (per path, in-memory) before succeeding —
// this is what makes internal/httpstep's retry/backoff and the engine's
// Saga compensation demonstrable against a real (if fake) service instead
// of only in unit tests.
package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
)

var (
	mu         sync.Mutex
	failCounts = make(map[string]int)
)

func echoHandler(w http.ResponseWriter, r *http.Request) {
	if failTarget, _ := strconv.Atoi(r.URL.Query().Get("fail")); failTarget > 0 {
		mu.Lock()
		failCounts[r.URL.Path]++
		seen := failCounts[r.URL.Path]
		mu.Unlock()
		if seen <= failTarget {
			log.Printf("%s: forced failure %d/%d", r.URL.Path, seen, failTarget)
			http.Error(w, "forced failure", http.StatusServiceUnavailable)
			return
		}
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var body map[string]any
	if len(data) > 0 {
		if err := json.Unmarshal(data, &body); err != nil {
			http.Error(w, "invalid JSON body: "+err.Error(), http.StatusBadRequest)
			return
		}
	}
	if body == nil {
		body = map[string]any{}
	}
	body["status"] = "ok"

	log.Printf("%s: %v", r.URL.Path, body)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

func main() {
	addr := flag.String("addr", ":9090", "Address to listen on")
	flag.Parse()

	mux := http.NewServeMux()
	for _, path := range []string{
		"/reserve", "/release",
		"/charge", "/refund",
		"/ship", "/cancel-ship",
		"/notify", "/notify-cancel",
		"/analytics", "/analytics-revert",
	} {
		mux.HandleFunc(path, echoHandler)
	}

	log.Printf("exampleservice listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
