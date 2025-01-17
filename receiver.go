package busdriver

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/redis/go-redis/v9"
)

type Event struct {
	Data []byte
}

type Handler func(ctx context.Context, event Event) error

type Receiver struct {
	client      *redis.Client
	subscriber  *redis.PubSub
	serviceName string
	handlers    map[string]Handler
	logger      *slog.Logger
	mu          sync.RWMutex
}

func NewReceiver(ctx context.Context, redisClient *redis.Client, serviceName string, logger *slog.Logger) *Receiver {
	return &Receiver{
		client:      redisClient,
		subscriber:  redisClient.Subscribe(ctx),
		serviceName: serviceName,
		handlers:    make(map[string]Handler),
		logger:      logger,
		mu:          sync.RWMutex{},
	}
}

func (r *Receiver) RegisterHandler(ctx context.Context, topic string, handler Handler) error {
	topic = MakeTopic(r.serviceName, topic)
	if err := r.subscriber.Subscribe(ctx, topic); err != nil {
		return fmt.Errorf("pub-sub subscribe: %v", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[topic] = handler

	return nil
}

func (r *Receiver) handlePayload(ctx context.Context, topic string, payload []byte) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	handler, ok := r.handlers[topic]
	if !ok {
		return fmt.Errorf("topic does not match")
	}

	var event Event
	if err := json.Unmarshal(payload, &event); err != nil {
		return fmt.Errorf("unmarshal payload error: %v", err)
	}

	if err := handler(ctx, event); err != nil {
		return fmt.Errorf("execution handler error: %v", err)
	}

	return nil
}

func (r *Receiver) Run(ctx context.Context) error {
	lg := r.logger.With("method", "Run")
	for {
		select {
		case msg := <-r.subscriber.Channel():
			if err := r.handlePayload(ctx, msg.Channel, []byte(msg.Payload)); err != nil {
				lg.
					With("topic", msg.Channel).
					Error("handlePayload failed", "error", err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
