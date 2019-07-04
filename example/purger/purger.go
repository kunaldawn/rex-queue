package main

import (
	"github.com/kunaldawn/rex-queue"
)

func main() {
	connection := rex_queue.OpenConnection("cleaner", "tcp", "localhost:6379", 2)
	queue := connection.OpenQueue("things")
	queue.PurgeReady()
}
