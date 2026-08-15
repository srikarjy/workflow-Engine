package steps

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/srikarjy/workflow_engine/internal/engine"
	"github.com/srikarjy/workflow_engine/internal/faultinject"
	"github.com/srikarjy/workflow_engine/internal/idempotency"
	"github.com/srikarjy/workflow_engine/internal/store"
)

// NotificationConfig holds configuration for a notification step.
type NotificationConfig struct {
	// ConfidenceThreshold is the minimum confidence required to send notification.
	// If the workflow's confidence is below this, the notification is skipped.
	ConfidenceThreshold float64 `json:"confidence_threshold,omitempty"`

	// Channels specifies which notification channels to use.
	// Supported: "email", "webhook", "slack"
	Channels []string `json:"channels"`

	// EmailConfig holds email-specific settings.
	EmailConfig *EmailNotificationConfig `json:"email_config,omitempty"`

	// WebhookConfig holds webhook-specific settings.
	WebhookConfig *WebhookNotificationConfig `json:"webhook_config,omitempty"`

	// SlackConfig holds Slack-specific settings.
	SlackConfig *SlackNotificationConfig `json:"slack_config,omitempty"`
}

// EmailNotificationConfig holds email notification settings.
type EmailNotificationConfig struct {
	To      []string `json:"to"`
	Subject string   `json:"subject"`
	Body    string   `json:"body"` // Template with placeholders
}

// WebhookNotificationConfig holds webhook notification settings.
type WebhookNotificationConfig struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"` // POST, PUT
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"` // Template with placeholders
}

// SlackNotificationConfig holds Slack notification settings.
type SlackNotificationConfig struct {
	WebhookURL string `json:"webhook_url"`
	Channel    string `json:"channel"`
	Text       string `json:"text"` // Template with placeholders
}

// notificationStep is a StepExecutor that sends notifications when confidence
// exceeds a threshold. It is idempotent via dedup key and never callable
// directly by agents - it must be invoked through the workflow engine.
type notificationStep struct {
	name   string
	config NotificationConfig
	store  store.EventLog
}

// NewNotificationStep returns a StepExecutor that sends notifications when
// the workflow confidence exceeds the configured threshold.
// The step is idempotent: re-running with the same dedup key will not
// send duplicate notifications.
// It must be invoked through the workflow engine (ExecuteWorkflow or
// ProcessStep), not called directly by agents.
func NewNotificationStep(name string, config NotificationConfig, s store.EventLog) engine.StepExecutor {
	return &notificationStep{
		name:   name,
		config: config,
		store:  s,
	}
}

func (s *notificationStep) Name() string { return s.name }

func (s *notificationStep) Execute(ctx context.Context, input map[string]any) (map[string]any, error) {
	// Extract confidence from input (set by previous steps)
	confidence, _ := input["confidence"].(float64)
	if confidence == 0 {
		// Try nested confidence structure
		if confMap, ok := input["confidence"].(map[string]any); ok {
			if overall, ok := confMap["overall"].(float64); ok {
				confidence = overall
			}
		}
	}

	// Check confidence threshold
	if s.config.ConfidenceThreshold > 0 && confidence < s.config.ConfidenceThreshold {
		log.Printf("step %s: confidence %.2f below threshold %.2f, skipping notification",
			s.name, confidence, s.config.ConfidenceThreshold)
		return map[string]any{
			"notification_skipped": true,
			"reason":               "confidence below threshold",
			"confidence":           confidence,
			"threshold":            s.config.ConfidenceThreshold,
		}, nil
	}

	// Build dedup key for idempotency
	// Include workflow ID and notification name to prevent duplicate sends
	wfID, _ := input["workflow_id"].(string)
	wfUUID, err := uuid.Parse(wfID)
	if err != nil {
		return nil, fmt.Errorf("notification step %s: invalid workflow_id %q: %w", s.name, wfID, err)
	}
	stepName := s.name
	dedupKey, err := idempotency.DedupKey(wfID, stepName, map[string]any{
		"notification": stepName,
		"confidence":   confidence,
	})
	if err != nil {
		return nil, fmt.Errorf("notification step %s: failed to create dedup key: %w", s.name, err)
	}

	// Check if already sent (idempotency)
	sent, err := s.store.HasNotificationSent(ctx, dedupKey)
	if err != nil {
		return nil, fmt.Errorf("notification step %s: failed to check notification status: %w", s.name, err)
	}
	if sent {
		log.Printf("step %s: notification already sent (dedup key: %s)", s.name, dedupKey)
		return map[string]any{
			"notification_skipped": true,
			"reason":               "already sent",
			"dedup_key":            dedupKey,
		}, nil
	}

	faultinject.Crash("notification_before_send")

	// Send notification via configured channels
	var results []map[string]any
	failures := 0
	for _, channel := range s.config.Channels {
		var result map[string]any
		var err error

		switch channel {
		case "email":
			result, err = s.sendEmail(ctx, input, s.config.EmailConfig)
		case "webhook":
			result, err = s.sendWebhook(ctx, input, s.config.WebhookConfig)
		case "slack":
			result, err = s.sendSlack(ctx, input, s.config.SlackConfig)
		default:
			result = map[string]any{"channel": channel, "status": "unsupported"}
			err = fmt.Errorf("unsupported channel: %s", channel)
		}

		if err != nil {
			failures++
			result = map[string]any{
				"channel": channel,
				"status":  "failed",
				"error":   err.Error(),
			}
		}
		if result == nil {
			result = map[string]any{"channel": channel, "status": "sent"}
		}
		results = append(results, result)
	}

	// If every channel failed, fail the step so the engine retries it;
	// nothing was delivered and no notification_sent event exists yet, so
	// a retry is safe. Partial success is recorded as sent (per-channel
	// outcomes are in the event payload) rather than retried, which would
	// double-send on the channels that succeeded.
	if len(s.config.Channels) > 0 && failures == len(s.config.Channels) {
		return nil, fmt.Errorf("notification step %s: all %d channels failed: %v", s.name, failures, results)
	}

	faultinject.Crash("notification_after_send_before_log")

	// Record notification sent event
	notificationEvent := store.Event{
		WorkflowID: wfUUID,
		StepName:   stepName,
		Type:       store.EventNotificationSent,
		DedupKey:   dedupKey,
		Payload: mustMarshal(map[string]any{
			"confidence":      confidence,
			"channels":        s.config.Channels,
			"results":         results,
			"notification_id": dedupKey,
		}),
	}
	_, err = s.store.AppendEvent(ctx, notificationEvent)
	if err != nil {
		return nil, fmt.Errorf("notification step %s: failed to record notification event: %w", s.name, err)
	}

	log.Printf("step %s: notification sent via %d channels (dedup: %s)",
		s.name, len(s.config.Channels), dedupKey)

	return map[string]any{
		"notification_sent": true,
		"dedup_key":         dedupKey,
		"confidence":        confidence,
		"channels":          s.config.Channels,
		"results":           results,
	}, nil
}

func (s *notificationStep) Compensate(ctx context.Context, output map[string]any) error {
	// Notification compensation is a no-op - we can't "unsend" a notification.
	// In a real system, you might send a follow-up "cancelled" notification.
	return nil
}

// sendEmail sends an email notification (stub implementation).
func (s *notificationStep) sendEmail(ctx context.Context, input map[string]any, config *EmailNotificationConfig) (map[string]any, error) {
	if config == nil {
		return nil, fmt.Errorf("email config not provided")
	}
	// In a real implementation, this would call an email service (SendGrid, SES, etc.)
	log.Printf("EMAIL to %v: %s - %s", config.To, config.Subject, config.Body)
	return map[string]any{"channel": "email", "status": "sent", "to": config.To}, nil
}

// sendWebhook sends a webhook notification (stub implementation).
func (s *notificationStep) sendWebhook(ctx context.Context, input map[string]any, config *WebhookNotificationConfig) (map[string]any, error) {
	if config == nil {
		return nil, fmt.Errorf("webhook config not provided")
	}
	// In a real implementation, this would make an HTTP request
	log.Printf("WEBHOOK %s %s: %s", config.Method, config.URL, config.Body)
	return map[string]any{"channel": "webhook", "status": "sent", "url": config.URL}, nil
}

// sendSlack sends a Slack notification (stub implementation).
func (s *notificationStep) sendSlack(ctx context.Context, input map[string]any, config *SlackNotificationConfig) (map[string]any, error) {
	if config == nil {
		return nil, fmt.Errorf("slack config not provided")
	}
	// In a real implementation, this would call Slack's webhook API
	log.Printf("SLACK to %s: %s", config.Channel, config.Text)
	return map[string]any{"channel": "slack", "status": "sent", "slack_channel": config.Channel}, nil
}

func mustMarshal(v any) json.RawMessage {
	data, _ := json.Marshal(v)
	return data
}
