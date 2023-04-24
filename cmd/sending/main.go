package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"time"

	"github.com/caarlos0/env"
	"github.com/joho/godotenv"
	"github.com/m3co/kafko"
	"github.com/m3co/kafko/log"
	"github.com/segmentio/kafka-go"
)

const (
	batchBytes   = 2 << 21
	batchSize    = 9
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

func main() { //nolint:funlen
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
	}).WithProcessDroppedMsg(func(_ *kafka.Message, log kafko.Logger) error {
		return nil
	})

	publisher := kafko.NewPublisher(log, opts)
	totalTasksChan := make(chan int)
	totalTasks := 0

	index := 0

	maxTaskAtOnce := make(chan struct{}, 400) //nolint:gomnd
	runningTasks := &sync.WaitGroup{}

	go func() {
		for v := range totalTasksChan {
			totalTasks += v

			runningTasks.Done()
		}
	}()

	for {
		runningTasks.Add(2) //nolint:gomnd

		go func(payload interface{}) {
			defer func() {
				totalTasksChan <- 1

				<-maxTaskAtOnce

				runningTasks.Done()
			}()

			for {
				if err := publisher.Publish(context.Background(), payload); err != nil {
					log.Errorf(err, "cannot publish")
					time.Sleep(time.Second)

					continue
				}

				break
			}
		}(
			map[string]interface{}{
				"id":     1,
				"update": true,
				"random": generateRandomBytes(10000), //nolint
			},
		)

		index++
		maxTaskAtOnce <- struct{}{}

		if index >= 250000 { //nolint:gomnd
			runningTasks.Wait()

			close(totalTasksChan)

			log.Printf("total tasks: %d", totalTasks)

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
