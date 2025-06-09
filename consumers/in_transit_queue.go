package consumers

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

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
			time.Sleep(5 * time.Second) // Slow processing to allow TTL expiration
			d.Ack(false)                // Manual acknowledgment
		}
	}()
}
