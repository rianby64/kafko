package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/caarlos0/env"
	"github.com/joho/godotenv"
	"github.com/m3co/kafko"
	"github.com/m3co/kafko/log"
	"github.com/segmentio/kafka-go"
)

const (
	maxBytes = 2 << 21
)

type Config struct {
	Name         string   `env:"NAME" envDefault:"name"`
	KafkaUser    string   `env:"KAFKA_USER"`
	KafkaPass    string   `env:"KAFKA_PASS"`
	KafkaTopic   string   `env:"KAFKA_TOPIC,required"`
	KafkaBrokers []string `env:"KAFKA_BROKERS,required"`
}

type MessageFromKafka struct {
	ID     int    `json:"id"`
	Update bool   `json:"update"`
	Random []byte `json:"random"`
}

func main() {
	log := log.NewLogger()
	cfg := loadConfig(log)

	opts := kafko.NewOptionsListener().WithReaderFactory(func() kafko.Reader {
		return kafka.NewReader(kafka.ReaderConfig{
			GroupID:     cfg.Name,
			Topic:       cfg.KafkaTopic,
			Brokers:     cfg.KafkaBrokers,
			Dialer:      kafko.NewDialer(cfg.KafkaUser, cfg.KafkaPass),
			ErrorLogger: log,
			MaxBytes:    maxBytes,
		})
	})

	consumer := kafko.NewListener(log, opts)

	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if err := consumer.Listen(ctx); err != nil {
			log.Panicf(err, "err := consumer.Listen(context.Background())")
		}
	}()

	totalTasks := 0
	msgChan, errChan := consumer.MessageAndErrorChannels()

	for msg := range msgChan {
		var msgFromKafka *MessageFromKafka

		if err := json.Unmarshal(msg, &msgFromKafka); err != nil {
			errChan <- err
		}

		totalTasks += msgFromKafka.ID

		fmt.Printf("\rtask: %d\r", totalTasks)

		errChan <- nil
	}
}

func loadConfig(log log.Logger) Config {
	if err := godotenv.Load(); err != nil {
		log.Panicf(err, "err := godotenv.Load()")
	}

	cfg := Config{}
	if err := env.Parse(&cfg); err != nil {
		log.Panicf(err, "err := env.Parse(&cfg)")
	}

	return cfg
}
