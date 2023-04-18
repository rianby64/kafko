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

#### Creating a Listener
To create a new listener, use the NewListener function:

```go
import (
	"github.com/m3co/kafko"
	"github.com/m3co/kafko/log"
	"github.com/segmentio/kafka-go"
)

logger := log.NewLogger()
opts := kafko.NewOptions().WithReaderFactory(func() kafko.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		GroupID: "your-group-id",
		Topic:   "your-topic",
		Brokers: []string{"broker1:9092", "broker2:9092"},
		Dialer:  kafko.NewDialer("username", "password"),
	})
})
listener := kafko.NewListener(logger, opts)
```

### Receiving Messages and Error Handling
To receive messages, use the MessageAndErrorChannels method and process messages in a loop:

```go
msgChan, errChan := listener.MessageAndErrorChannels()

for {
	msg := <-msgChan
	log.Printf("Received message: %s", string(msg))

	errChan <- nil
}
```

Provide an error to `errChan` in order to prevent Kafko to commit the message passed at `msgChan`. E.g. `msgChan` contains a JSON you want to save into MongoDB but MongoDB is down, therefore the `msgChan` should be processed later. In this case, pass the error to `errChan`.

#### Graceful Shutdown
To perform a graceful shutdown, use the Shutdown method:

```go
ctx := context.Background()
err := listener.Shutdown(ctx)
```

#### Configuration
Kafko provides several options for customization:

WithReaderFactory: Set a custom reader factory for advanced use cases
WithProcessDroppedMsg: Sets the dropped message processing handler for the Options instance
For example:

```go
opts := kafko.NewOptions().
	WithReaderFactory(yourReaderFactory).
	WithProcessDroppedMsg(yourProcessDroppedMsgHandler)

listener := kafko.NewListener(logger, opts)
```

### Kafka Producer (Publisher)

#### Creating a Publisher
To create a new publisher, use the NewPublisher function:

```go
import (
	"github.com/m3co/kafko"
	"github.com/m3co/kafko/log"
)

logger := log.NewLogger()
publisher := kafko.NewPublisher(logger)
```

#### Publishing Messages
To publish messages, use the Publish method:

```go
ctx := context.Background()
payload := struct {
	Message string
}{
	Message: "Hello, world!",
}

err := publisher.Publish(ctx, payload)
```

#### Error Handling
Kafko provides built-in error handling for dropped messages. You can customize the behavior by providing your own processDroppedMsg function when creating a publisher:

```go
import (
	"github.com/m3co/kafko"
	"github.com/m3co/kafko/log"
	"github.com/segmentio/kafka-go"
)

processMsg := func(msg *kafka.Message, log kafko.Logger) error {
	// Your custom error handling logic here
	return nil
}

logger := log.NewLogger()
opts := kafko.NewOptionsPublisher().
	WithProcessDroppedMsg(processMsg)

publisher := kafko.NewPublisher(logger, opts)
```

#### Graceful Shutdown
To perform a graceful shutdown, use the Shutdown method:

```go
ctx := context.Background()
err := publisher.Shutdown(ctx)
```

#### Configuration
Kafko provides several options for customization:

* WithWriterFactory: Set a custom writer factory for advanced use cases
* WithProcessDroppedMsg: Set a custom function to handle dropped messages, for example:

```go
opts := kafko.NewOptionsPublisher().
	WithWriterFactory(yourWriterFactory).
	WithProcessDroppedMsg(yourDroppedMsgHandler)

publisher := kafko.NewPublisher(logger, opts)
```

#### Testing
To run the test suite, simply execute the following command in the project's root directory:

```bash
go test ./...
```

## Contributing
Contributions to Kafko are welcome! If you find a bug or would like to request a new feature, please open an issue on the GitHub repository. For code contributions, please submit a pull request.

## License
Kafko is released under the MIT License.
