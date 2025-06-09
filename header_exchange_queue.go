package main

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

func DeclareAndBindHeaderExchangeQueue(ch *amqp.Channel, queueName string, exchangeName string, queueDeclareArgs amqp.Table, queueBindArgs amqp.Table) (*amqp.Queue, error) {
	// Declare queue for Headers Exchange with DLX
	queue, err := ch.QueueDeclare(
		queueName,        // name
		true,             // durable
		false,            // delete when unused
		false,            // exclusive
		false,            // no-wait
		queueDeclareArgs, // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare %s: %v", queueName, err)
	}

	err = ch.QueueBind(
		queue.Name,    // queue name
		"",            // routing key (empty for headers)
		exchangeName,  // exchange
		false,         // no-wait
		queueBindArgs, // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("failed to bind %s: %v", queueName, err)
	}

	return &queue, nil
}
