package main

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

func DeclareAndBindTopicExchangeQueue(ch *amqp.Channel, queueName string, exchangeName string, bindingKey string) (*amqp.Queue, error) {
	// Declare queue for Topic Exchange
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
		bindingKey,   // binding key
		exchangeName, // exchange
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("failed to bind %s: %v", queueName, err)
	}

	return &queue, nil
}
