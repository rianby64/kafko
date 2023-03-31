package main

import (
	"context"

	"github.com/caarlos0/env"
	"github.com/joho/godotenv"
	"github.com/m3co/kafko"
	"github.com/m3co/kafko/log"
	"github.com/segmentio/kafka-go"
)

type Config struct {
	Name         string   `env:"NAME" envDefault:"name"`
	KafkaUser    string   `env:"KAFKA_USER"`
	KafkaPass    string   `env:"KAFKA_PASS"`
	KafkaTopic   string   `env:"KAFKA_TOPIC,required"`
	KafkaBrokers []string `env:"KAFKA_BROKERS,required"`
}

func handleMessage(log log.Logger, consumer *kafko.Listener) {
	msg, errChan := consumer.MessageAndErrorChannels()

	log.Printf("Received message: %s", string(<-msg))

	errChan <- nil
}

func main() {
	log := log.NewLogger()
	cfg := loadConfig(log)

	opts := kafko.NewOptions().WithReaderFactory(func() kafko.Reader {
		return kafka.NewReader(kafka.ReaderConfig{
			GroupID:     cfg.Name,
			Topic:       cfg.KafkaTopic,
			Brokers:     cfg.KafkaBrokers,
			Dialer:      kafko.NewDialer(cfg.KafkaUser, cfg.KafkaPass),
			Logger:      log,
			ErrorLogger: log,
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

	for {
		handleMessage(log, consumer)
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
