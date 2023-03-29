package main

import (
	"context"

	"github.com/caarlos0/env"
	"github.com/joho/godotenv"
	kafkame "github.com/rianby64/kafko"
	"github.com/rianby64/kafko/log"
)

type Config struct {
	Name         string   `env:"NAME" envDefault:"name"`
	KafkaUser    string   `env:"KAFKA_USER"`
	KafkaPass    string   `env:"KAFKA_PASS"`
	KafkaTopic   string   `env:"KAFKA_TOPIC,required"`
	KafkaBrokers []string `env:"KAFKA_BROKERS,required"`
}

func main() {
	log := log.NewLogger()
	cfg := loadConfig(log)

	opts := kafkame.NewOptions()
	consumer := kafkame.NewListener(
		cfg.KafkaUser,
		cfg.KafkaPass,
		cfg.Name,
		cfg.KafkaTopic,
		cfg.KafkaBrokers,
		log,
		opts,
	)

	go func() {
		if err := consumer.Listen(context.Background()); err != nil {
			log.Panicf(err, "err := consumer.Listen(context.Background())")
		}
	}()

	for {
		msg, errChan := consumer.MessageAndErrorChannels()
		log.Printf(string(<-msg))
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
