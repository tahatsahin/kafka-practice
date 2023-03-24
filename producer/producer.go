package producer

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"strconv"
	"time"
)

const (
	topic          = "message-log"
	broker1Address = "localhost:9092"
	broker2Address = "localhost:9093"
	broker3Address = "localhost:9094"
)

func getKafkaWriter(topic string, kafkaURL ...string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func Produce(ctx context.Context) {
	// initialize counter
	i := 0

	// initialize the writer with the broker addresses and the topic
	kafkaWriter := getKafkaWriter(topic, broker1Address, broker2Address, broker3Address)

	defer func(kafkaWriter *kafka.Writer) {
		err := kafkaWriter.Close()
		if err != nil {
			panic(err)
		}
	}(kafkaWriter)

	for {
		// each kafka message has a key and value. key is used to decide
		// which partition (and consequently, which broker)
		// the message gets published on
		err := kafkaWriter.WriteMessages(ctx, kafka.Message{
			Key: []byte(strconv.Itoa(i)),
			// create an arbitrary message payload for the value
			Value: []byte("this is message" + strconv.Itoa(i)),
		})
		if err != nil {
			panic("could not write message: " + err.Error())
		}

		// log a confirmation once the message is written
		fmt.Println("writes: ", i)
		i++
		// sleep for a second
		time.Sleep(time.Second)

	}
}
