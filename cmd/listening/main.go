package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kafko"
	"kafko/log"

	"github.com/caarlos0/env/v9"
	"github.com/segmentio/kafka-go"
)

type Config struct {
	KafkaGroupID string   `env:"KAFKA_GROUP"`
	KafkaUser    string   `env:"KAFKA_USER"`
	KafkaPass    string   `env:"KAFKA_PASS"`
	KafkaTopic   string   `env:"KAFKA_TOPIC,required"`
	KafkaBrokers []string `env:"KAFKA_BROKERS,required"`
}

type ListenerHandler struct {
	log     log.Logger
	showLog bool
}

func (handler *ListenerHandler) Handle(_ context.Context, msg *kafka.Message) error {
	if !handler.showLog {
		return nil
	}

	payload := string(msg.Value)

	handler.log.Printf("OK (%s) (partition=%d)", payload, msg.Partition)

	return nil
}

func (handler *ListenerHandler) ToggleLoggin() {
	handler.showLog = !handler.showLog
}

func NewListenerHandler(log log.Logger) *ListenerHandler {
	return &ListenerHandler{
		showLog: true,
		log:     log,
	}
}

func main() {
	log := log.NewLogger()
	cfg := loadConfig(log)
	breakListenLoop := make(chan struct{}, 1)
	shutdown := make(chan os.Signal, 1)

	signal.Notify(shutdown, syscall.SIGTERM, syscall.SIGINT)

	listenerHandler := NewListenerHandler(log)
	opts := kafko.NewOptionsListener().WithReaderFactory(func() kafko.Reader {
		return kafka.NewReader(kafka.ReaderConfig{
			GroupID:     cfg.KafkaGroupID,
			Topic:       cfg.KafkaTopic,
			Brokers:     cfg.KafkaBrokers,
			Dialer:      kafko.NewDialer(cfg.KafkaUser, cfg.KafkaPass),
			ErrorLogger: log,
			//Logger:      log,
		})
	}).WithHandler(listenerHandler)

	consumer := kafko.NewListener(log, opts)
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	go func() {
		for {
			select {
			case <-breakListenLoop:
				return
			default:
			}

			if err := consumer.Listen(ctx); err != nil {
				log.Errorf(err, "err := consumer.Listen(ctx)")
			}

			time.Sleep(time.Second)
		}
	}()

	showHandlerLogChan := make(chan os.Signal, 1)
	signal.Notify(showHandlerLogChan, syscall.SIGUSR1)

	go func() {
		for range showHandlerLogChan {
			listenerHandler.ToggleLoggin()
		}
	}()

	<-shutdown
	close(breakListenLoop)

	log.Printf("shutting down")

	if err := consumer.Shutdown(ctx); err != nil {
		log.Errorf(err, "err := consumer.Shutdown(ctx)")
	}

	log.Printf("bye")
}

func loadConfig(log log.Logger) Config {
	cfg := Config{}
	if err := env.Parse(&cfg); err != nil {
		log.Panicf(err, "err := env.Parse(&cfg)")
	}

	return cfg
}
