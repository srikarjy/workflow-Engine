// Package faultinject provides deterministic, synchronous crash points used
// by cmd/faultinject to verify crash-recovery behavior. Reaching a
// checkpoint whose name matches the FAULT_INJECT environment variable
// self-terminates the process with SIGKILL immediately, in-line with the
// code under test. This replaces guessing a crash point from outside the
// process via a timed sleep-then-kill, which cannot reliably land on a
// specific step in the execution sequence.
package faultinject

import (
	"os"
	"syscall"
)

// Crash terminates the current process with SIGKILL if the FAULT_INJECT
// environment variable equals point. It is a no-op otherwise, so production
// code paths that call it pay only an env lookup.
func Crash(point string) {
	if point == "" || os.Getenv("FAULT_INJECT") != point {
		return
	}
	_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
	select {} // unreachable once SIGKILL is delivered; blocks just in case
}
