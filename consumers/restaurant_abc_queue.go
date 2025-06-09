package consumers

import (
	"encoding/json"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"math/rand"
	"time"
)

func ConsumeRestaurantABCQueue(ch *amqp.Channel) {
	err := ch.Qos(
		1,     // prefetch count (process 1 message at a time)
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		log.Fatalf("Failed to set QoS for restaurant_abc_queue: %v", err)
	}
	orderDeliveries, err := ch.Consume(
		"restaurant_abc_queue", // queue
		"",                     // consumer tag
		false,                  // auto-ack (manual acknowledgment)
		false,                  // exclusive
		false,                  // no-local
		false,                  // no-wait
		nil,                    // args
	)
	if err != nil {
		log.Fatalf("Failed to register consumer for restaurant_abc_queue: %v", err)
	}

	go handleOrderDeliveryMessages(orderDeliveries)
}

func handleOrderDeliveryMessages(orderDeliveries <-chan amqp.Delivery) {
	for d := range orderDeliveries {
		log.Printf("Received order delivery message with body: %s", d.Body)
		time.Sleep(2 * time.Second) // Simulate processing

		var order map[string]string
		json.Unmarshal(d.Body, &order)

		// Poison message always fails
		if order["order_id"] == "order_id_POISON" {
			log.Printf("Processing order delivery message id: %s failed for poison message, requeueing", order["order_id"])
			d.Nack(false, true) // Negative acknowledgment with requeue
			continue
		}

		// Get a chance to show the case handle order failed
		if rand.Intn(10) < 3 { // 30% chance to fail
			log.Printf("Processing order delivery message id: %s failed, requeueing", order["order_id"])
			d.Nack(false, true) // Negative acknowledgment with requeue
			continue
		}

		// Handle an order delivery message successfully
		log.Printf("Order delivery message id: %s processed successfully", order["order_id"])
		d.Ack(false) // Manual acknowledgment
	}
}
