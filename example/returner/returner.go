package main

import (
	"github.com/kunaldawn/rex-queue"
	"log"
)

func main() {
	connection := rex_queue.OpenConnection("returner", "tcp", "localhost:6379", 2)
	queue := connection.OpenQueue("things")
	returned := queue.ReturnAllRejected()
	log.Printf("queue returner returned %d rejected deliveries", returned)
}
