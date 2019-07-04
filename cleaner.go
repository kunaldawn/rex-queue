package rex_queue

import "fmt"

type Cleaner struct {
	connection *redisConnection
}

func NewCleaner(connection *redisConnection) *Cleaner {
	return &Cleaner{connection: connection}
}

func (cleaner *Cleaner) Clean() error {
	connectionNames := cleaner.connection.GetConnections()
	for _, connectionName := range connectionNames {
		connection := cleaner.connection.hijackConnection(connectionName)
		if connection.Check() {
			// skip active connections!
			continue
		}

		if err := cleaner.CleanConnection(connection); err != nil {
			return err
		}
	}

	return nil
}

func (cleaner *Cleaner) CleanConnection(connection *redisConnection) error {
	queueNames := connection.GetConsumingQueues()
	for _, queueName := range queueNames {
		queue, ok := connection.OpenQueue(queueName).(*redisQueue)
		if !ok {
			return fmt.Errorf("cleaner failed to open queue %s", queueName)
		}

		cleaner.CleanQueue(queue)
	}

	if !connection.Close() {
		return fmt.Errorf("cleaner failed to close connection %s", connection)
	}

	if err := connection.CloseAllQueuesInConnection(); err != nil {
		return fmt.Errorf("cleaner failed to close all queues %s %s", connection, err)
	}

	return nil
}

func (cleaner *Cleaner) CleanQueue(queue *redisQueue) {
	returned := queue.ReturnAllUnacked()
	queue.CloseInConnection()
	_ = returned
}
