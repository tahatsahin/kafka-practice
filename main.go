package main

import (
	"context"
	"kafka-in-go/consumer"
	"kafka-in-go/producer"
)

func main() {
	ctx := context.Background()

	go producer.Produce(ctx)
	consumer.Consume(ctx)
}
