package publishers

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

func DeclareAndBindFanoutExchangeQueue(ch *amqp.Channel, queueName string, exchangeName string) (*amqp.Queue, error) {
	// Declare queues for Fanout Exchange
	queue, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare %s: %v", queueName, err)
	}
	err = ch.QueueBind(
		queue.Name,   // queue name
		"",           // routing key (empty for fanout)
		exchangeName, // exchange
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("failed to bind %s: %v", queueName, err)
	}

	return &queue, nil
}
