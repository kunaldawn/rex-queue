package rex_queue

type Consumer interface {
	Consume(delivery Delivery)
}

type ConsumerFunc func(Delivery)

func (consumerFunc ConsumerFunc) Consume(delivery Delivery) {
	consumerFunc(delivery)
}
