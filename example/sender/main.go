package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"time"

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

	sender := busdriver.NewSender(
		redisClient,
		serviceName,
		logger.With("component", "busdriver:sender"),
		busdriver.SetBufferSize(5),
	)

	for i := range 10 {
		data, err := entity.NewValuer(i).Marshal()
		if err != nil {
			log.Fatalln(err)
		}

		if err := sender.Send(ctx, topic, busdriver.Event{Data: data}); err != nil {
			log.Fatalln(err)
		}

		log.Printf("sent counter %d", i)
		time.Sleep(time.Second * 3)
	}
}
