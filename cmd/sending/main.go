package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/caarlos0/env"
	"github.com/joho/godotenv"
	"github.com/m3co/kafko"
	"github.com/m3co/kafko/log"
	"github.com/segmentio/kafka-go"
)

const (
	batchBytes   = 2 << 21
	batchSize    = 50
	batchTimeout = time.Second * 3
)

type Config struct {
	Name         string   `env:"NAME"`
	KafkaUser    string   `env:"KAFKA_USER,required"`
	KafkaPass    string   `env:"KAFKA_PASS,required"`
	KafkaTopic   string   `env:"KAFKA_TOPIC,required"`
	KafkaBrokers []string `env:"KAFKA_BROKERS,required"`
}

func generateRandomBytes(n int) []byte {
	bytes := make([]byte, n)
	_, err := rand.Read(bytes)

	if err != nil {
		fmt.Println(err) //nolint:forbidigo

		return nil
	}

	return bytes
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
	})

	publisher := kafko.NewPublisher(log, opts)

	index := 0

	maxTaskAtOnce := make(chan struct{}, 400) //nolint:gomnd

	for {
		go func(payload map[string]interface{}, index int) {
			defer func() {
				<-maxTaskAtOnce
			}()

			start := time.Now()

			if err := publisher.Publish(context.Background(), payload); err != nil {
				log.Errorf(err, "err := publisher.Publish(context.Background(), payload)")
			}

			log.Printf("sent... %d (%v)", index, time.Since(start))
		}(map[string]interface{}{
			"id":     index,
			"update": true,
			"random": generateRandomBytes(15000), //nolint
		}, index)

		index++
		maxTaskAtOnce <- struct{}{}

		if index >= 5000000 { //nolint:gomnd
			time.Sleep(10 * time.Second) //nolint:gomnd

			break
		}
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
