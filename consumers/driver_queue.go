package consumers

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

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
