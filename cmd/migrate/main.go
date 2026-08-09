// Command migrate applies or reverts database migrations in migrations/.
package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func dsn() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://workflow:workflow@localhost:15432/workflow?sslmode=disable"
}

func main() {
	direction := "up"
	if len(os.Args) > 1 {
		direction = os.Args[1]
	}

	m, err := migrate.New("file://migrations", dsn())
	if err != nil {
		fmt.Fprintln(os.Stderr, "migrate: init failed:", err)
		os.Exit(1)
	}

	switch direction {
	case "up":
		err = m.Up()
	case "down":
		err = m.Down()
	default:
		fmt.Fprintf(os.Stderr, "migrate: unknown direction %q (want up|down)\n", direction)
		os.Exit(1)
	}

	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
	fmt.Println("migrate: done")
}
