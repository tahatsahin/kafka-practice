package consumer

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"time"
)

const (
	topic          = "message-log"
	broker1Address = "localhost:9092"
	broker2Address = "localhost:9093"
	broker3Address = "localhost:9094"
)

func Consume(ctx context.Context) {
	// initialize a new reader with the brokers and topic
	// the groupID identifies the consumer and prevents
	// it from receiving duplicate messages
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker1Address, broker2Address, broker3Address},
		Topic:    topic,
		GroupID:  "my-group",
		MinBytes: 500,
		MaxBytes: 1e6,
		MaxWait:  3 * time.Second,
	})
	for {
		// the `ReadMessage` method blocks until we receive the next event
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			panic("could not read message " + err.Error())
		}
		// after receiving the message, log its value
		fmt.Println("received: ", string(msg.Value))
	}
}
