package rex_queue

type BatchConsumer interface {
	Consume(batch Deliveries)
}
