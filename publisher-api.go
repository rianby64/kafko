package kafko

import (
	"context"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
)

func (publisher *Publisher) lastError() error {
	publisher.stateErrorLock.RLock()

	defer publisher.stateErrorLock.RUnlock()

	return publisher.stateError
}

func (publisher *Publisher) setStateError(err error) {
	publisher.stateErrorLock.Lock()

	defer publisher.stateErrorLock.Unlock()

	publisher.stateError = err
}

func (publisher *Publisher) clearStateError() {
	publisher.stateErrorLock.Lock()

	defer publisher.stateErrorLock.Unlock()

	publisher.stateError = nil
}

/*
	I want to expose here a simple strategy in order to deal with
	some errors we're experiencing in the network...

	if publisher.stateError {                                               // OK
		launch a recovery strategy                                          // OK
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
		set publisher.stateError to NO-error,                               // OK
		so other attempts will take place as expected                       // OK

		return // so break the loop                                         // OK
	}
*/

func (publisher *Publisher) processError(err error, msg *kafka.Message) (bool, error) {
	publisher.processErrorLock.Lock()

	defer publisher.processErrorLock.Unlock()

	if lastError := publisher.lastError(); lastError != nil {
		if err := publisher.opts.processDroppedMsg(msg, publisher.log); err != nil {
			return true, errors.Wrap(err, "cannot process dropped message")
		}

		return true, nil
	}

	publisher.setStateError(err)
	publisher.log.Errorf(err, "cannot write message to Kafka")

	return false, nil
}

// Publish accepts only one payload, is concurrent-safe. Call it from different places at the same time.
func (publisher *Publisher) Publish(ctx context.Context, payload []byte) error {
	shouldClearStateBeforeReturning := true
	key := publisher.opts.keyGenerator.Generate()
	msg := kafka.Message{
		Key:   key,
		Value: payload,
	}

	if lastError := publisher.lastError(); lastError != nil {
		if err := publisher.opts.processDroppedMsg(&msg, publisher.log); err != nil {
			return errors.Wrap(err, "cannot process dropped message")
		}

		return nil
	}

	defer func() {
		if !shouldClearStateBeforeReturning {
			return
		}

		publisher.clearStateError()
	}()

	for {
		if err := publisher.writer.WriteMessages(ctx, msg); err != nil {
			if shoulExitFromLoop, err := publisher.processError(err, &msg); shoulExitFromLoop {
				shouldClearStateBeforeReturning = false

				return err
			}

			publisher.opts.backoffStrategy.Wait()

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
