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

	"github.com/srikarjy/workflow_engine/internal/engine"
	"github.com/srikarjy/workflow_engine/internal/store"
	"github.com/srikarjy/workflow_engine/internal/workflowdef"
)

func main() {
	var postgresDSN string
	var rootCmd = &cobra.Command{
		Use:           "workflow-cli",
		Short:         "Workflow engine CLI",
		SilenceUsage:  true,
		SilenceErrors: true,
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
			s, err := store.New(ctx, postgresDSN)
			if err != nil {
				return err
			}
			defer s.Close()

			workflows, err := s.ListWorkflows(ctx, 50)
			if err != nil {
				return err
			}

			fmt.Println("ID                                    | NAME        | STATUS       | CREATED")
			fmt.Println("--------------------------------------|-------------|--------------|---------------------")
			for _, w := range workflows {
				fmt.Printf("%s | %-11s | %-12s | %s\n", w.ID.String(), w.Name, w.Status, w.CreatedAt)
			}
			return nil
		},
	}

	var runCmd = &cobra.Command{
		Use:   "run",
		Short: "Run a YAML workflow definition to completion (or compensation on failure)",
		RunE: func(cmd *cobra.Command, args []string) error {
			workflowFile, _ := cmd.Flags().GetString("workflow")
			inputFile, _ := cmd.Flags().GetString("input")
			resume, _ := cmd.Flags().GetString("resume")

			def, err := workflowdef.Load(workflowFile)
			if err != nil {
				return err
			}

			var input map[string]any
			if inputFile != "" {
				data, err := os.ReadFile(inputFile)
				if err != nil {
					return err
				}
				if err := json.Unmarshal(data, &input); err != nil {
					return err
				}
			}

			wfID := uuid.New()
			if resume != "" {
				wfID, err = uuid.Parse(resume)
				if err != nil {
					return fmt.Errorf("--resume: %w", err)
				}
			}

			ctx := context.Background()
			s, err := store.New(ctx, postgresDSN)
			if err != nil {
				return err
			}
			defer s.Close()

			e := engine.NewEngine(s, nil, "cli-run", nil)
			fmt.Printf("running workflow %s (%s)\n", wfID, def.Name)
			_, err = e.ExecuteWorkflow(ctx, wfID, def.Build(), input)
			if err != nil {
				fmt.Printf("workflow %s failed and compensated: %v\n", wfID, err)
				return err
			}
			fmt.Printf("workflow %s completed\n", wfID)
			return nil
		},
	}
	runCmd.Flags().String("workflow", "", "YAML workflow definition file (required)")
	runCmd.Flags().String("input", "", "JSON file with workflow input")
	runCmd.Flags().String("resume", "", "Resume a previously started workflow by ID instead of starting a new one")
	_ = runCmd.MarkFlagRequired("workflow")

	rootCmd.AddCommand(createCmd, statusCmd, listCmd, runCmd)

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

var pool *pgxpool.Pool

func mustMarshal(v any) json.RawMessage {
	data, _ := json.Marshal(v)
	return data
}
