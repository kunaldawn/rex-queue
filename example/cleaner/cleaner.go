package main

import (
	"github.com/kunaldawn/rex-queue"
	"time"
)

func main() {
	connection := rex_queue.OpenConnection("cleaner", "tcp", "localhost:6379", 2)
	cleaner := rex_queue.NewCleaner(connection)

	for _ = range time.Tick(time.Second) {
		cleaner.Clean()
	}
}
