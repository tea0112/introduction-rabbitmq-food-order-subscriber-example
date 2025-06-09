package publishers

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

func DeclareAndBindDeadLetterExchangeQueue(ch *amqp.Channel) {
	// Declare Dead Letter Exchange
	err := ch.ExchangeDeclare(
		"dlx_exchange", // name
		"direct",       // type
		true,           // durable
		false,          // auto-deleted
		false,          // internal
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare dlx_exchange: %v", err)
	}

	// Declare Dead Letter Queue
	dlq, err := ch.QueueDeclare(
		"dlq", // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare dlq: %v", err)
	}

	// Bind DLQ to Dead Letter Exchange
	err = ch.QueueBind(
		dlq.Name,       // queue name
		"dlq_key",      // routing key
		"dlx_exchange", // exchange
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		log.Fatalf("Failed to bind dlq: %v", err)
	}
}
