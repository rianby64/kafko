package kafko

import (
	"crypto/tls"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

const (
	dialerTimeout  = time.Second * time.Duration(10)
	waitNextAtempt = time.Second * time.Duration(3)
)

func NewDialer(username, password string) *kafka.Dialer {
	dialer := &kafka.Dialer{
		Timeout:   dialerTimeout,
		DualStack: true,
		KeepAlive: time.Minute,
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
