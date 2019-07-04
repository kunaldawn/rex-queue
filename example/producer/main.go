package main

import (
	"fmt"
	"github.com/kunaldawn/rex-queue"
	"log"
	"time"
)

const (
	numDeliveries = 10000000
	batchSize     = 5
)

func main() {
	connection := rex_queue.OpenConnection("producer", "tcp", "localhost:6379", 2)
	things := connection.OpenQueue("things")
	balls := connection.OpenQueue("balls")
	var before time.Time

	delivery := ""
	i := 0
	for i = 0; i < numDeliveries; i++ {
		delivery = fmt.Sprintf("data %d", i)
		things.Publish(delivery)

		if i%batchSize == 0 {
			duration := time.Now().Sub(before)
			before = time.Now()
			perSecond := time.Second / (duration / batchSize)
			log.Printf("produced %d %s [%d/ps]", i, delivery, perSecond)
			balls.Publish("ball")
		}
	}

	duration := time.Now().Sub(before)
	before = time.Now()
	perSecond := time.Second / (duration / batchSize)
	log.Printf("produced %d %s [%d/ps]", i, delivery, perSecond)
}
