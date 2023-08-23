package kafko

import (
	"context"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
)

func (publisher *Publisher) Publish(ctx context.Context, payload []byte) error {
	/* I want to expose here a simple strategy in order to deal with
	   some errors we're experiencing in the network...

		if publisher.stateError {
			launch a recovery strategy
				- write in the disk
				- send to an available kafka topic
				- write to mongoDB
				- ...
				- log this thing
				- who cares!!!

			return
		}

		writer := publisher.writer
	    for {
			if err := writer.WriteMessage(); err != nil {
				ATTENTION:  the first failer MUST block other attempts,
				            therefore other running attempts must fall into
							the recovery strategy

				set publisher.stateError to error,
				so other attempts to write will be redirected to
				the recovery strategy

				wait some timeout then
				if err := writer.Close(); err != nil {
					just log the error, and we know that here we've lost
					the current batch of messages, so, bad luck.
				}

				publisher.writer = opts.writerFactory() // build a new connection
				continue // in order to repeat this attempt, at least 3 times
			}

			// if you are here
			set publisher.stateError to NO-error,
			so other attempts will take place as expected

			return // so break the loop
		}
	*/

	writer := publisher.writer
	if err := writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(uuid.New().String()),
		Value: payload,
	}); err != nil {
		return errors.Wrapf(err, "cannot write message to Kafka")
	}

	return nil
}

// Shutdown method to perform a graceful shutdown.
func (publisher *Publisher) Shutdown(_ context.Context) error {
	return publisher.closeWriter()
}
