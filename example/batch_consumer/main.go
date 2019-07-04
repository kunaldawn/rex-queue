package main

import (
	"github.com/kunaldawn/rex-queue"
	"log"
	"time"
)

const unackedLimit = 1000

func main() {
	connection := rex_queue.OpenConnection("consumer", "tcp", "localhost:6379", 2)

	queue := connection.OpenQueue("things")
	queue.StartConsuming(unackedLimit, time.Second)
	queue.AddBatchConsumer("things", 111, NewBatchConsumer("things"))

	queue = connection.OpenQueue("balls")
	queue.StartConsuming(unackedLimit, time.Second)
	queue.AddBatchConsumer("balls", 111, NewBatchConsumer("balls"))

	select {}
}

type BatchConsumer struct {
	tag string
}

func NewBatchConsumer(tag string) *BatchConsumer {
	return &BatchConsumer{tag: tag}
}

func (consumer *BatchConsumer) Consume(batch rex_queue.Deliveries) {
	time.Sleep(time.Millisecond)
	log.Printf("%s consumed %d: %s", consumer.tag, len(batch), batch[0])
	batch.Ack()
}
