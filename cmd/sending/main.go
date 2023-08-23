package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/caarlos0/env"
	"github.com/joho/godotenv"
	"github.com/m3co/kafko"
	"github.com/m3co/kafko/log"
	"github.com/segmentio/kafka-go"
)

const (
	batchBytes   = 2 << 21
	batchSize    = 100
	batchTimeout = time.Second * 5
)

type Config struct {
	Name         string   `env:"NAME"`
	KafkaUser    string   `env:"KAFKA_USER,required"`
	KafkaPass    string   `env:"KAFKA_PASS,required"`
	KafkaTopic   string   `env:"KAFKA_TOPIC,required"`
	KafkaBrokers []string `env:"KAFKA_BROKERS,required"`
}

func publishIndex(publisher *kafko.Publisher, index int, log *log.LoggerInternal) {
	payload := map[string]interface{}{
		"id": index,
	}

	msg, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}

	if err := publisher.Publish(context.Background(), msg); err != nil {
		log.Errorf(err, "cannot publish")
	}
}

func main() {
	log := log.NewLogger()
	cfg := loadConfig(log)

	opts := kafko.NewOptionsPublisher().WithWriterFactory(func() kafko.Writer {
		writer := &kafka.Writer{
			Addr:        kafka.TCP(cfg.KafkaBrokers...),
			Topic:       cfg.KafkaTopic,
			ErrorLogger: log,
			// Logger:       log,
			BatchBytes:   batchBytes,
			BatchSize:    batchSize,
			BatchTimeout: batchTimeout,
		}

		writer.AllowAutoTopicCreation = true

		return writer
	}).WithProcessDroppedMsg(func(_ *kafka.Message, log kafko.Logger) error {
		return nil
	})

	publisher := kafko.NewPublisher(log, opts)

	tasksAtOnce := make(chan struct{}, 500)

	for index := 0; index < 10000000; index++ {
		go func(index int) {
			defer func() {
				<-tasksAtOnce
			}()
			publishIndex(publisher, index, log)
		}(index)

		tasksAtOnce <- struct{}{}
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
