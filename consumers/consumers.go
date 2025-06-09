package consumers

import (
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"math/rand"
	"strings"
	"time"
)

func Consume(ch *amqp.Channel) {
	// Consumer for restaurant_abc_queue with prefetch count
	ConsumeRestaurantABCQueue(ch)

	// Consumer for driver1_queue
	ConsumeDriver1Queue(ch)

	// Consumer for driver2_queue
	ConsumeDriver2Queue(ch)

	// Consumer for north_deliveries
	ConsumeNorthDeliveriesQueue(ch)

	// Consumer for in_transit_queue with prefetch count to demo TTL
	ConsumeInTransitQueue(ch)

	// Consumer for Dead Letter Queue
	ConsumeDeadLetterQueue(ch)
}

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

func ConsumeDriver1Queue(ch *amqp.Channel) {
	driver1Deliveries, err := ch.Consume(
		"driver1_queue",
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to register consumer for driver1_queue: %v", err)
	}

	go func() {
		for d := range driver1Deliveries {
			log.Printf("Driver 1 received order ready message with body: %s", d.Body)
			time.Sleep(time.Second) // Simulate processing
			d.Ack(false)            // Manual acknowledgment
		}
	}()
}

func ConsumeDriver2Queue(ch *amqp.Channel) {
	driver2Deliveries, err := ch.Consume(
		"driver2_queue",
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to register consumer for driver2_queue: %v", err)
	}

	go func() {
		for d := range driver2Deliveries {
			log.Printf("Driver 2 received order ready message with body: %s", d.Body)
			time.Sleep(time.Second) // Simulate processing
			d.Ack(false)            // Manual acknowledgment
		}
	}()
}

func ConsumeNorthDeliveriesQueue(ch *amqp.Channel) {
	northDeliveries, err := ch.Consume(
		"north_deliveries",
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to register consumer for north_deliveries: %v", err)
	}

	go func() {
		for d := range northDeliveries {
			log.Printf("North region received assignment message with body: %s", d.Body)
			time.Sleep(time.Second) // Simulate processing
			d.Ack(false)            // Manual acknowledgment
		}
	}()
}

func ConsumeInTransitQueue(ch *amqp.Channel) {
	err := ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		log.Fatalf("Failed to set QoS for in_transit_queue: %v", err)
	}
	statusDeliveries, err := ch.Consume(
		"in_transit_queue",
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to register consumer for in_transit_queue: %v", err)
	}

	go func() {
		for d := range statusDeliveries {
			log.Printf("Received status update message with body: %s", d.Body)
			time.Sleep(10 * time.Second) // Slow processing to allow TTL expiration
			d.Ack(false)                 // Manual acknowledgment
		}
	}()
}

func ConsumeDeadLetterQueue(ch *amqp.Channel) {
	dlqDeliveries, err := ch.Consume(
		"dlq",
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to register consumer for Dead Letter Queue: %v", err)
	}

	go func() {
		for d := range dlqDeliveries {
			log.Printf("Received dead-lettered message with body: %s", d.Body)
			// Check x-death header for retry/expiration history
			if deaths, ok := d.Headers["x-death"].([]any); ok {
				var xDeathRecords strings.Builder
				for i, death := range deaths {
					if deathMap, ok := death.(amqp.Table); ok {
						xDeathRecords.WriteString(fmt.Sprintf("Death %d: exchange=%s, count=%d\n",
							i+1, deathMap["exchange"], deathMap["count"]))
					}
				}
				if xDeathRecords.Len() > 0 {
					log.Printf("x-death Records:\n%s", xDeathRecords.String())
				}
			}
			d.Ack(false) // Acknowledge dead-lettered message
		}
	}()
}
