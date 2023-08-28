package kafko

import (
	"context"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
)

func (publisher *Publisher) Publish(ctx context.Context, payload []byte) error {
	/*
		I want to expose here a simple strategy in order to deal with
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

		for {                                                                   // OK
			if err := writer.WriteMessage(); err != nil {                       // OK
				ATTENTION:  the first failer MUST block other attempts,
							therefore other running attempts must fall into
							the recovery strategy

				set publisher.stateError to error,
				so other attempts to write will be redirected to
				the recovery strategy

				wait some timeout then                                          // OK
				continue // in order to repeat this attempt, at least 3 times   // OK
			}

			// if you are here
			set publisher.stateError to NO-error,
			so other attempts will take place as expected

			return // so break the loop
		}
	*/

	key := publisher.opts.keyGenerator.Generate()

	for {
		if err := publisher.writer.WriteMessages(ctx, kafka.Message{
			Key:   key,
			Value: payload,
		}); err != nil {
			publisher.log.Errorf(err, "cannot write message to Kafka")

			if publisher.opts.backoffStrategy != nil {
				publisher.opts.backoffStrategy.Wait()
			}

			continue
		}

		break
	}

	return nil
}

// Shutdown method to perform a graceful shutdown.
func (publisher *Publisher) Shutdown(_ context.Context) error {
	if err := publisher.closeWriter(); err != nil {
		return errors.Wrapf(err, "cannot close kafka connection")
	}

	return nil
}
