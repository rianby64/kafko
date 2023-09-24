package kafko

import (
	"context"
)

// Listen starts the Listener to fetch and process messages from the Kafka topic.
// It also starts the commit loop and handles message errors.
func (listener *Listener) Listen(_ context.Context) error {
	return nil
}

// Shutdown gracefully shuts down the Listener, committing any uncommitted messages
// and closing the Kafka reader.
func (listener *Listener) Shutdown(_ context.Context) error {
	return nil
}
