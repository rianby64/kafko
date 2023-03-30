package main

import (
	"context"
	"time"

	"github.com/caarlos0/env"
	"github.com/joho/godotenv"
	"github.com/rianby64/kafko"
	"github.com/rianby64/kafko/log"
)

const (
	sleepPublish = time.Millisecond * time.Duration(100)
)

type Config struct {
	Name         string   `env:"NAME"`
	KafkaUser    string   `env:"KAFKA_USER,required"`
	KafkaPass    string   `env:"KAFKA_PASS,required"`
	KafkaTopic   string   `env:"KAFKA_TOPIC,required"`
	KafkaBrokers []string `env:"KAFKA_BROKERS,required"`
}

func main() {
	log := log.NewLogger()
	cfg := loadConfig(log)

	publisher := kafko.NewPublisher(
		func() kafko.Writer {
			writer := kafko.NewWriter(
				cfg.KafkaUser,
				cfg.KafkaPass,
				cfg.KafkaTopic,
				cfg.KafkaBrokers,
			)

			return writer
		},
		log,
	)

	index := 0

	for {
		time.Sleep(sleepPublish)

		payload := map[string]interface{}{
			"id": 0,
		}

		if err := publisher.Publish(context.Background(), payload); err != nil {
			log.Errorf(err, "err := publisher.Publish(context.Background(), payload)")

			continue
		}

		log.Printf("sent...")
		index++
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
