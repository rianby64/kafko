package main

import (
	"context"
	"encoding/json"
	"time"

	"kafko"
	"kafko/log"

	"github.com/caarlos0/env/v9"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
)

const (
	batchBytes   = 2 << 21
	batchSize    = 50
	batchTimeout = time.Second * 5
)

type Config struct {
	Name         string   `env:"NAME"`
	KafkaUser    string   `env:"KAFKA_USER,required"`
	KafkaPass    string   `env:"KAFKA_PASS,required"`
	KafkaTopic   string   `env:"KAFKA_TOPIC,required"`
	KafkaBrokers []string `env:"KAFKA_BROKERS,required"`
}

func publishIndex(publisher *kafko.Publisher, index int) error {
	payload := map[string]interface{}{
		"id": index,
	}

	msg, err := json.Marshal(payload)
	if err != nil {
		return errors.Wrap(err, "cannot marshal")
	}

	if err := publisher.Publish(context.Background(), msg); err != nil {
		return errors.Wrap(err, "cannot publish")
	}

	return nil
}

type handlerProcessDroppedMsg struct {
	log kafko.Logger
}

func (handler *handlerProcessDroppedMsg) Handle(ctx context.Context, msg *kafka.Message) error {
	handler.log.Printf("handle process dropped msg: %s", string(msg.Value))

	select {
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "handling error")
	case <-time.After(batchTimeout):
		handler.log.Printf("waited some timeout... I already saved all the necessary info")

		return nil
	}
}

func main() {
	log := log.NewLogger()
	cfg := loadConfig(log)

	const maxTaskAtOnce = 1000

	opts := kafko.NewOptionsPublisher().
		WithWriterFactory(func() kafko.Writer {
			writer := &kafka.Writer{
				Addr:         kafka.TCP(cfg.KafkaBrokers...),
				Topic:        cfg.KafkaTopic,
				ErrorLogger:  log,
				BatchBytes:   batchBytes,
				BatchSize:    batchSize,
				BatchTimeout: batchTimeout,
				Logger:       log,
			}

			writer.AllowAutoTopicCreation = true

			return writer
		}).
		WithProcessDroppedMsg(&handlerProcessDroppedMsg{
			log: log,
		})

	publisher := kafko.NewPublisher(log, opts)
	tasksAtOnce := make(chan struct{}, maxTaskAtOnce)

	for index := 0; index < 10000000; index++ {
		go func(index int) {
			defer func() {
				<-tasksAtOnce
			}()

			err := publishIndex(publisher, index)
			if err != nil {
				log.Errorf(err, "something went wrong")
			}
		}(index)

		tasksAtOnce <- struct{}{}
	}
}

func loadConfig(log log.Logger) Config {
	cfg := Config{}
	if err := env.Parse(&cfg); err != nil {
		log.Panicf(err, "err := env.Parse(&cfg)")
	}

	return cfg
}
