package main

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"introduction-rabbitmq-food-order-subscriber-example/consumers"
	"introduction-rabbitmq-food-order-subscriber-example/exchange_queues"
	"log"
)

func main() {
	// Connect to RabbitMQ server
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	// Open a channel
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	exchange_queues.DeclareAndBind(ch)
	consumers.Consume(ch)

	// Keep the application running
	select {}
}
