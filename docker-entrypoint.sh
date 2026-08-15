#!/bin/sh
# Applies migrations (idempotent: golang-migrate no-ops if there's nothing
# new) then starts the worker. Render (and most single-process PaaS hosts)
# only run one command, so this is the one process that has to do both.
set -e
migrate up
exec worker "$@"
