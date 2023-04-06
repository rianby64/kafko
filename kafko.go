package kafko

import (
	"crypto/tls"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

const (
	dialerTimeout     = time.Duration(10) * time.Second
	commitInterval    = time.Duration(30) * time.Second
	reconnectInterval = time.Duration(10) * time.Second
	processingTimeout = time.Duration(5) * time.Second
)

func NewDialer(username, password string) *kafka.Dialer {
	dialer := &kafka.Dialer{
		Timeout:   dialerTimeout,
		DualStack: true,
	}

	if username != "" && password != "" {
		mechanism := plain.Mechanism{
			Username: username,
			Password: password,
		}

		dialer.SASLMechanism = mechanism
		dialer.TLS = &tls.Config{
			MinVersion: tls.VersionTLS12,
			ClientAuth: tls.NoClientCert,
		}
	}

	return dialer
}

func NewWriter(username, password, topic string, brokers []string) *kafka.Writer {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:          brokers,
		Topic:            topic,
		Dialer:           NewDialer(username, password),
		CompressionCodec: kafka.Zstd.Codec(),
	})

	writer.AllowAutoTopicCreation = true

	return writer
}
