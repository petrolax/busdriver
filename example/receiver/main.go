package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/redis/go-redis/v9"

	"github.com/petrolax/busdriver"
	"github.com/petrolax/busdriver/example/entity"
)

func main() {
	topic := "aaa"
	serviceName := "aservice"
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	rec := busdriver.NewReceiver(ctx, redisClient, serviceName, logger.With("component", "busdriver:receiver"))
	if err := rec.RegisterHandler(ctx, topic, entity.ValuerHandler); err != nil {
		logger.Error(err.Error())
		return
	}

	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := rec.Run(cancelCtx); err != nil {
		logger.Error(err.Error())
	}
}
