package main

import (
	"context"
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
	batchTimeout = time.Second * 3
)

type Config struct {
	Name         string   `env:"NAME"`
	KafkaUser    string   `env:"KAFKA_USER,required"`
	KafkaPass    string   `env:"KAFKA_PASS,required"`
	KafkaTopic   string   `env:"KAFKA_TOPIC,required"`
	KafkaBrokers []string `env:"KAFKA_BROKERS,required"`
}

type Duration struct {
	log *log.LoggerInternal
}

func (d *Duration) Observe(duration float64) {
	durationT := time.Duration(duration) * time.Millisecond
	d.log.Printf("duration of writeMessage: %v", durationT)
}

func main() {
	log := log.NewLogger()
	cfg := loadConfig(log)

	opts := kafko.NewOptionsPublisher().WithWriterFactory(func() kafko.Writer {
		writer := kafka.NewWriter(kafka.WriterConfig{
			Brokers:          cfg.KafkaBrokers,
			Topic:            cfg.KafkaTopic,
			Dialer:           kafko.NewDialer(cfg.KafkaUser, cfg.KafkaPass),
			CompressionCodec: kafka.Zstd.Codec(),
			ErrorLogger:      log,
			Logger:           log,
			BatchBytes:       batchBytes,
			BatchSize:        batchSize,
			BatchTimeout:     batchTimeout,
		})

		writer.AllowAutoTopicCreation = true

		return writer
	}).WithMetricDurationProcess(&Duration{
		log: log,
	})

	publisher := kafko.NewPublisher(log, opts)

	index := 0

	for {
		payload := map[string]interface{}{
			"id":     index,
			"update": true,
		}

		if err := publisher.Publish(context.Background(), payload); err != nil {
			log.Errorf(err, "err := publisher.Publish(context.Background(), payload)")

			continue
		}

		log.Printf("sent... %d", index)
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
