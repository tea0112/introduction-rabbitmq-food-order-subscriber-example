package consumers

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"strings"
)

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
			log.Printf("Received Dead Letter message with body: %s", d.Body)

			// Check x-death header for retry/expiration history
			if deaths, ok := d.Headers["x-death"].([]any); ok {
				var xDeathRecords strings.Builder
				for i, death := range deaths {
					if deathMap, ok := death.(amqp.Table); ok {
						xDeathRecords.WriteString(fmt.Sprintf("Death message %d: exchange=%s, queue=%s, rounting-keys=%s, count=%d, reason=%s, body=%s\n",
							i+1, deathMap["exchange"], deathMap["queue"], deathMap["routing-keys"], deathMap["count"], deathMap["reason"], d.Body))
					}
				}

				if xDeathRecords.Len() > 0 {
					log.Printf("x-death Records:\n%s", xDeathRecords.String())
				}
			}

			d.Ack(false) // Acknowledge a Dead Letter message
		}
	}()
}
