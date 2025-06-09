package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
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

	// Declare Dead Letter Exchange
	err = ch.ExchangeDeclare(
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

	// Declare queue for Direct Exchange
	args := amqp.Table{
		"x-queue-type":              "quorum",       // Queue type supports x-delivery-limit
		"x-delivery-limit":          3,              // Retry up to 3 times
		"x-dead-letter-exchange":    "dlx_exchange", // Dead letter exchange
		"x-dead-letter-routing-key": "dlq_key",      // Dead letter routing key
	}
	restaurantQueue, err := DeclareAndBindDirectExchangeQueue(ch, "restaurant_abc_queue", "orders_exchange", "restaurant_abc", args)
	if err != nil {
		log.Fatalf(err.Error())
	}

	// Declare queues for Fanout Exchange
	driver1Queue, err := DeclareAndBindFanoutExchangeQueue(ch, "driver1_queue", "order_ready_exchange")
	if err != nil {
		log.Fatalf(err.Error())
	}
	driver2Queue, err := DeclareAndBindFanoutExchangeQueue(ch, "driver2_queue", "order_ready_exchange")
	if err != nil {
		log.Fatalf(err.Error())
	}

	// Declare queue for Topic Exchange
	northQueue, err := DeclareAndBindTopicExchangeQueue(ch, "north_deliveries", "delivery_exchange", "delivery.assign.north")
	if err != nil {
		log.Fatalf(err.Error())
	}

	// Declare queue for Headers Exchange with DLX
	statusArgs := amqp.Table{
		"x-dead-letter-exchange":    "dlx_exchange", // Dead letter exchange
		"x-dead-letter-routing-key": "dlq_key",      // Dead letter routing key
	}
	headers := amqp.Table{
		"status": "in_transit", // header for routing
	}
	inTransitQueue, err := DeclareAndBindHeaderExchangeQueue(ch, "in_transit_queue", "status_exchange", statusArgs, headers)
	if err != nil {
		log.Fatalf(err.Error())
	}

	// Consumer for restaurant_abc_queue with prefetch count
	err = ch.Qos(
		1,     // prefetch count (process 1 message at a time)
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		log.Fatalf("Failed to set QoS for restaurant_abc_queue: %v", err)
	}
	orderDeliveries, err := ch.Consume(
		restaurantQueue.Name, // queue
		"",                   // consumer tag
		false,                // auto-ack (manual acknowledgment)
		false,                // exclusive
		false,                // no-local
		false,                // no-wait
		nil,                  // args
	)
	if err != nil {
		log.Fatalf("Failed to register consumer for restaurant_abc_queue: %v", err)
	}
	go func() {
		for d := range orderDeliveries {
			log.Printf("Received order: %s", d.Body)
			time.Sleep(2 * time.Second) // Simulate processing
			var order map[string]string
			json.Unmarshal(d.Body, &order)
			// Poison message always fails
			if order["order_id"] == "poison" {
				log.Printf("Processing failed for poison message, requeueing")
				d.Nack(false, true) // Negative acknowledgment with requeue
			} else if rand.Intn(10) < 3 { // 30% chance to fail
				log.Printf("Processing failed, requeueing")
				d.Nack(false, true) // Negative acknowledgment with requeue
			} else {
				log.Printf("Order processed successfully")
				d.Ack(false) // Manual acknowledgment
			}
		}
	}()

	// Consumer for driver1_queue
	driver1Deliveries, err := ch.Consume(
		driver1Queue.Name,
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
			log.Printf("Driver 1 received order ready: %s", d.Body)
			time.Sleep(time.Second) // Simulate processing
			d.Ack(false)            // Manual acknowledgment
		}
	}()

	// Consumer for driver2_queue
	driver2Deliveries, err := ch.Consume(
		driver2Queue.Name,
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
			log.Printf("Driver 2 received order ready: %s", d.Body)
			time.Sleep(time.Second) // Simulate processing
			d.Ack(false)            // Manual acknowledgment
		}
	}()

	// Consumer for north_deliveries
	northDeliveries, err := ch.Consume(
		northQueue.Name,
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
			log.Printf("North region received assignment: %s", d.Body)
			time.Sleep(time.Second) // Simulate processing
			d.Ack(false)            // Manual acknowledgment
		}
	}()

	// Consumer for in_transit_queue with prefetch count to demo TTL
	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		log.Fatalf("Failed to set QoS for in_transit_queue: %v", err)
	}
	statusDeliveries, err := ch.Consume(
		inTransitQueue.Name,
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
			log.Printf("Received status update: %s", d.Body)
			time.Sleep(10 * time.Second) // Slow processing to allow TTL expiration
			d.Ack(false)                 // Manual acknowledgment
		}
	}()

	// Consumer for Dead Letter Queue
	dlqDeliveries, err := ch.Consume(
		dlq.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to register consumer for dlq: %v", err)
	}
	go func() {
		for d := range dlqDeliveries {
			log.Printf("Received dead-lettered message: %s", d.Body)
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

	// Keep the application running
	select {}
}
