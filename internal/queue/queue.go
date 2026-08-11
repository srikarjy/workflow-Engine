// Package queue implements the Redis Streams step-dispatch consumer/producer.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// StepMessage represents a workflow step dispatched via Redis Streams.
type StepMessage struct {
	WorkflowID string         `json:"workflow_id"`
	StepName   string         `json:"step_name"`
	Input      map[string]any `json:"input"`
	DedupKey   string         `json:"dedup_key"`
	RetryCount int            `json:"retry_count"`
}

// Client wraps a Redis client for stream operations.
type Client struct {
	rdb    *redis.Client
	stream string
	group  string
}

// NewClient creates a new queue client.
func NewClient(rdb *redis.Client, stream, group string) *Client {
	return &Client{rdb: rdb, stream: stream, group: group}
}

// EnsureGroup creates the consumer group if it doesn't exist.
func (c *Client) EnsureGroup(ctx context.Context) error {
	err := c.rdb.XGroupCreateMkStream(ctx, c.stream, c.group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}

// ProduceStep adds a step message to the stream.
func (c *Client) ProduceStep(ctx context.Context, msg StepMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return c.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: c.stream,
		Values: map[string]any{"data": string(data)},
	}).Err()
}

// ConsumeSteps reads steps from the stream using the consumer group.
func (c *Client) ConsumeSteps(ctx context.Context, consumer string, count int64, block time.Duration) ([]redis.XMessage, error) {
	streams, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    c.group,
		Consumer: consumer,
		Streams:  []string{c.stream, ">"},
		Count:    count,
		Block:    block,
	}).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	if len(streams) == 0 {
		return nil, nil
	}
	return streams[0].Messages, nil
}

// Ack acknowledges a processed message.
func (c *Client) Ack(ctx context.Context, msgID string) error {
	return c.rdb.XAck(ctx, c.stream, c.group, msgID).Err()
}

// Pending returns pending messages for the consumer group.
func (c *Client) Pending(ctx context.Context) ([]redis.XPendingExt, error) {
	return c.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: c.stream,
		Group:  c.group,
		Start:  "-",
		End:    "+",
		Count:  100,
	}).Result()
}

// Claim claims pending messages for a consumer.
func (c *Client) Claim(ctx context.Context, consumer string, minIdleTime time.Duration, msgIDs ...string) ([]redis.XMessage, error) {
	return c.rdb.XClaim(ctx, &redis.XClaimArgs{
		Stream:   c.stream,
		Group:    c.group,
		Consumer: consumer,
		MinIdle:  minIdleTime,
		Messages: msgIDs,
	}).Result()
}

// ReclaimPending transfers ownership of any message that has been pending
// (delivered but unacked) for at least minIdle to consumer, and returns
// them for (re)processing. This is how a replacement worker picks up a
// message whose original consumer crashed before acking it: XREADGROUP with
// ">" only ever delivers messages nobody has claimed yet, so without this a
// crashed consumer's in-flight message would sit in the group's pending
// entries list forever.
func (c *Client) ReclaimPending(ctx context.Context, consumer string, minIdle time.Duration) ([]redis.XMessage, error) {
	pending, err := c.Pending(ctx)
	if err != nil {
		return nil, err
	}

	var ids []string
	for _, p := range pending {
		if p.Idle >= minIdle {
			ids = append(ids, p.ID)
		}
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return c.Claim(ctx, consumer, minIdle, ids...)
}

// ParseStepMessage parses a StepMessage from a Redis stream message.
func ParseStepMessage(msg redis.XMessage) (StepMessage, error) {
	var step StepMessage
	data, ok := msg.Values["data"].(string)
	if !ok {
		return StepMessage{}, ErrInvalidMessage
	}
	err := json.Unmarshal([]byte(data), &step)
	return step, err
}

// ErrInvalidMessage is returned when a stream message cannot be parsed.
var ErrInvalidMessage = errors.New("queue: invalid message format")
