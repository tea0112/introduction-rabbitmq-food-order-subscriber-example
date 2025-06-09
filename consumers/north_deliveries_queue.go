package consumers

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

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
