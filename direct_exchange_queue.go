package main

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

func DeclareAndBindDirectExchangeQueue(ch *amqp.Channel, queueName string, exchangeName string, bindingKey string, args amqp.Table) (*amqp.Queue, error) {
	// Declare queue for Direct Exchange with delivery limit and DLX
	queue, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		args,      // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare %s: %v", queueName, err)
	}

	// Bind queue to Direct Exchange
	err = ch.QueueBind(
		queue.Name,   // queue name
		bindingKey,   // routing key
		exchangeName, // exchange
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("failed to bind %s: %v", queueName, err)
	}

	return &queue, nil
}
