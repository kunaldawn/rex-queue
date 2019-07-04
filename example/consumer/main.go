package main

import (
	"fmt"
	"github.com/kunaldawn/rex-queue"
	"log"
	"time"
)

const (
	unackedLimit = 1000
	numConsumers = 1
	batchSize    = 5
)

func main() {
	connection := rex_queue.OpenConnection("consumer", "tcp", "localhost:6379", 2)
	queue := connection.OpenQueue("things")
	queue.StartConsuming(unackedLimit, 500*time.Millisecond)
	for i := 0; i < numConsumers; i++ {
		name := fmt.Sprintf("consumer %d", i)
		queue.AddConsumer(name, NewConsumer(i))
	}
	select {}
}

type Consumer struct {
	name   string
	count  int
	before time.Time
}

func NewConsumer(tag int) *Consumer {
	return &Consumer{
		name:   fmt.Sprintf("consumer%d", tag),
		count:  0,
		before: time.Now(),
	}
}

func (consumer *Consumer) Consume(delivery rex_queue.Delivery) {
	consumer.count++
	if consumer.count%batchSize == 0 {
		duration := time.Now().Sub(consumer.before)
		consumer.before = time.Now()
		perSecond := time.Second / (duration / batchSize)
		log.Printf("CONSUME : [%s] : %d :  %s [%d/ps]", consumer.name, consumer.count, delivery.Payload(), perSecond)
	}
	delivery.Ack()
}
