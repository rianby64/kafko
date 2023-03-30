package kafko_test

import (
	"context"
	"fmt"
	"testing"

	kafkame "github.com/rianby64/kafko"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

type WriterMock1 struct {
	Msg string
}

func (writer *WriterMock1) Close() error {
	return nil
}

func (writer *WriterMock1) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	for _, msg := range msgs {
		writer.Msg += string(msg.Value)
	}

	return nil
}

func Test_Publish_no_error_OK(t *testing.T) {
	t.Parallel()

	log := &LoggerMock{}
	writer := &WriterMock1{}
	expectedMsg := "message"
	publisher := kafkame.NewPublisher(func() kafkame.Writer {
		return writer
	}, log)

	err := publisher.Publish(context.TODO(), expectedMsg)

	assert.Nil(t, err)
	assert.Equal(t, fmt.Sprintf(`"%s"`, expectedMsg), writer.Msg)
}
