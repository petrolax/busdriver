package busdriver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	defaultBufferLifetime = time.Minute * 5
)

var (
	ErrorEmptyBuffer = errors.New("buffer is empty")
)

type Option func(s *Sender)

func SetBufferSize(bufferSize int64) Option {
	return func(s *Sender) {
		s.bufferSize = bufferSize

		if s.bufferLifetime == 0 {
			s.bufferLifetime = defaultBufferLifetime
		}
	}
}

func SetBufferLifetime(lifetime time.Duration) Option {
	return func(s *Sender) {
		s.bufferLifetime = lifetime
	}
}

type Sender struct {
	client         *redis.Client
	serviceName    string
	bufferSize     int64
	bufferLifetime time.Duration
	logger         *slog.Logger
}

func NewSender(client *redis.Client, serviceName string, logger *slog.Logger, opts ...Option) *Sender {
	sender := &Sender{
		client:      client,
		serviceName: serviceName,
		logger:      logger,
	}

	for _, opt := range opts {
		opt(sender)
	}
	return sender
}

func (s *Sender) updateBufferLifetime(ctx context.Context, topic string) error {
	expireRes := s.client.Expire(ctx, topic, s.bufferLifetime)
	if err := expireRes.Err(); err != nil {
		return fmt.Errorf("update expire: %w", err)
	}
	if !expireRes.Val() {
		return fmt.Errorf("Setup expire time false")
	}
	return nil
}

func (s *Sender) addToBuffer(ctx context.Context, topic string, event []byte) error {
	if s.bufferSize == 0 {
		return ErrorEmptyBuffer
	}

	res := s.client.LLen(ctx, topic)
	if res.Err() != nil {
		return fmt.Errorf("get list len: %w", res.Err())
	}

	if res.Val() >= s.bufferSize {
		for _ = range (res.Val() - s.bufferSize) + 1 {
			if err := s.client.RPop(ctx, topic).Err(); err != nil {
				return fmt.Errorf("clear list: %w", err)
			}
		}
	}

	if err := s.client.LPush(ctx, topic, event).Err(); err != nil {
		return fmt.Errorf("push to list: %w", err)
	}

	if err := s.updateBufferLifetime(ctx, topic); err != nil {
		return fmt.Errorf("update buffer lifetime: %w", err)
	}

	return nil
}

func (s *Sender) getValuesFromBuffer(ctx context.Context, topic string) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		for {
			res := s.client.RPop(ctx, topic)

			value, err := res.Bytes() // возвращает ту же самую ошибку, что и res.Err()
			if err != nil {
				break
			}

			if !yield(value) {
				return
			}
		}
	}
}

func (s *Sender) sendFromBuffer(ctx context.Context, topic string) error {
	if s.bufferSize == 0 {
		return nil
	}

	s.getValuesFromBuffer(ctx, topic)(
		func(value []byte) bool {
			if len(value) == 0 {
				return false
			}

			if err := s.send(ctx, topic, value); err != nil {
				return false
			}

			return true
		},
	)
	return nil
}

func (s *Sender) send(ctx context.Context, topic string, data []byte) error {
	res := s.client.Publish(ctx, topic, data)
	if res.Err() != nil {
		return fmt.Errorf("publish: %w", res.Err())
	}

	if res.Val() == 0 {
		if err := s.addToBuffer(ctx, topic, data); err != nil {
			return fmt.Errorf("add to buffer: %w", err)
		}
	}
	return nil
}

func (s *Sender) Send(ctx context.Context, topic string, event Event) error {
	topic = MakeTopic(s.serviceName, topic)

	data, err := json.Marshal(&event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	if err = s.send(ctx, topic, data); err != nil {
		if errors.Is(err, ErrorEmptyBuffer) {
			return fmt.Errorf("zero subscribers")
		}
		return fmt.Errorf("send event: %w", err)
	}

	if err = s.sendFromBuffer(ctx, topic); err != nil {
		s.logger.
			With("method", "Send", "topic").
			Error("send from buffer got error",
				"error", err,
			)
	}

	return nil
}
