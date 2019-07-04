package main

import (
	"fmt"
	"github.com/kunaldawn/rex-queue"
	"log"
	"net/http"
)

func main() {
	connection := rex_queue.OpenConnection("handler", "tcp", "localhost:6379", 2)
	http.Handle("/overview", NewHandler(connection))
	fmt.Printf("Handler listening on http://localhost:3333/overview\n")
	http.ListenAndServe(":3333", nil)
}

type Handler struct {
	connection rex_queue.Connection
}

func NewHandler(connection rex_queue.Connection) *Handler {
	return &Handler{connection: connection}
}

func (handler *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	layout := request.FormValue("layout")
	refresh := request.FormValue("refresh")

	queues := handler.connection.GetOpenQueues()
	stats := handler.connection.CollectStats(queues)
	log.Printf("queue stats\n%s", stats)
	fmt.Fprint(writer, stats.GetHtml(layout, refresh))
}
