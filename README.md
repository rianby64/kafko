# Kafko

Kafko is a simple and easy-to-use Go library for consuming and producing messages with Apache Kafka. It provides a clean and intuitive interface, allowing you to focus on your application logic without worrying about the underlying Kafka implementation details.

## Features

- Clean and easy-to-understand API for consuming and producing messages
- Support for user authentication with username and password
- Flexible configuration options for Kafka consumers and producers
- Customizable logger implementation
- Interfaces for Kafka reader and writer, allowing you to provide your own custom implementations if needed

## Installation

```bash
go get -u github.com/m3co/kafko
```

## Usage

### Kafka Consumer (Listener)

```go
package main

import (
	"context"
	"github.com/m3co/kafko"
	"github.com/segmentio/kafka-go"
)

func handleMessage(log log.Logger, consumer *kafko.Listener) {
	msg, errChan := consumer.MessageAndErrorChannels()

	log.Printf("Received message: %s", string(<-msg))

	errChan <- nil
}

func main() {
    // Create a new Listener with the desired configuration
    opts := kafko.NewOptions().WithReaderFactory(func() kafko.Reader {
        return kafka.NewReader(kafka.ReaderConfig{
            GroupID:   "your-group-id",
            Topic:     "your-topic",
            Brokers:   []string{"broker1:9092", "broker2:9092"},
            Dialer:    kafko.NewDialer("username", "password"),
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
```

### Kafka Producer (Notifier)

[TODO]

## Contributing
Contributions to Kafko are welcome! If you find a bug or have a feature request, feel free to open an issue or submit a pull request.

## License
Kafko is released under the MIT License.
