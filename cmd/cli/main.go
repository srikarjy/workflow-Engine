// Command cli provides workflow inspection and submission tools.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/srikarjy/workflow_engine/internal/store"
)

func main() {
	var postgresDSN string
	var rootCmd = &cobra.Command{
		Use:   "workflow-cli",
		Short: "Workflow engine CLI",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			var err error
			pool, err = pgxpool.New(context.Background(), postgresDSN)
			return err
		},
		PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
			if pool != nil {
				pool.Close()
			}
			return nil
		},
	}

	rootCmd.PersistentFlags().StringVar(&postgresDSN, "postgres", "postgres://workflow:workflow@localhost:15432/workflow?sslmode=disable", "PostgreSQL DSN")

	var createCmd = &cobra.Command{
		Use:   "create",
		Short: "Create a new workflow",
		RunE: func(cmd *cobra.Command, args []string) error {
			file, _ := cmd.Flags().GetString("file")
			name, _ := cmd.Flags().GetString("name")

			var input map[string]any
			if file != "" {
				data, err := os.ReadFile(file)
				if err != nil {
					return err
				}
				if err := json.Unmarshal(data, &input); err != nil {
					return err
				}
			}

			ctx := context.Background()
			s, err := store.New(ctx, postgresDSN)
			if err != nil {
				return err
			}
			defer s.Close()

			wfID := uuid.New()
			if err := s.CreateWorkflow(ctx, wfID, name, mustMarshal(input)); err != nil {
				return err
			}
			fmt.Println(wfID.String())
			return nil
		},
	}
	createCmd.Flags().String("file", "", "JSON file with workflow input")
	createCmd.Flags().String("name", "default", "Workflow name")

	var statusCmd = &cobra.Command{
		Use:   "status",
		Short: "Get workflow status",
		RunE: func(cmd *cobra.Command, args []string) error {
			idStr, _ := cmd.Flags().GetString("id")
			wfID, err := uuid.Parse(idStr)
			if err != nil {
				return err
			}

			ctx := context.Background()
			s, err := store.New(ctx, postgresDSN)
			if err != nil {
				return err
			}
			defer s.Close()

			wf, err := s.GetWorkflow(ctx, wfID)
			if err != nil {
				return err
			}

			events, err := s.ReplayEvents(ctx, wfID)
			if err != nil {
				return err
			}

			output, _ := json.MarshalIndent(map[string]any{
				"workflow": wf,
				"events":   events,
			}, "", "  ")
			fmt.Println(string(output))
			return nil
		},
	}
	statusCmd.Flags().String("id", "", "Workflow ID")

	var listCmd = &cobra.Command{
		Use:   "list",
		Short: "List workflows",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			rows, err := pool.Query(ctx, `SELECT id, name, status, created_at FROM workflows ORDER BY created_at DESC LIMIT 50`)
			if err != nil {
				return err
			}
			defer rows.Close()

			fmt.Println("ID                                    | NAME        | STATUS       | CREATED")
			fmt.Println("--------------------------------------|-------------|--------------|---------------------")
			for rows.Next() {
				var id uuid.UUID
				var name, status string
				var created string
				if err := rows.Scan(&id, &name, &status, &created); err != nil {
					return err
				}
				fmt.Printf("%s | %-11s | %-12s | %s\n", id.String(), name, status, created)
			}
			return nil
		},
	}

	rootCmd.AddCommand(createCmd, statusCmd, listCmd)

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

var pool *pgxpool.Pool

func mustMarshal(v any) json.RawMessage {
	data, _ := json.Marshal(v)
	return data
}
