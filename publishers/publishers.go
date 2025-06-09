package publishers

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

func DeclareAndBind(ch *amqp.Channel) {
	DeclareAndBindDeadLetterExchangeQueue(ch)

	// Declare queue for Direct Exchange
	args := amqp.Table{
		"x-queue-type":              "quorum",       // Queue type supports x-delivery-limit
		"x-delivery-limit":          3,              // Retry up to 3 times
		"x-dead-letter-exchange":    "dlx_exchange", // Dead letter exchange
		"x-dead-letter-routing-key": "dlq_key",      // Dead letter routing key
	}
	_, err := DeclareAndBindDirectExchangeQueue(ch, "restaurant_abc_queue", "orders_exchange", "restaurant_abc", args)
	if err != nil {
		log.Fatalf(err.Error())
	}

	// Declare queues for Fanout Exchange
	_, err = DeclareAndBindFanoutExchangeQueue(ch, "driver1_queue", "order_ready_exchange")
	if err != nil {
		log.Fatalf(err.Error())
	}
	_, err = DeclareAndBindFanoutExchangeQueue(ch, "driver2_queue", "order_ready_exchange")
	if err != nil {
		log.Fatalf(err.Error())
	}

	// Declare queue for Topic Exchange
	_, err = DeclareAndBindTopicExchangeQueue(ch, "north_deliveries", "delivery_exchange", "delivery.assign.north")
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
	_, err = DeclareAndBindHeaderExchangeQueue(ch, "in_transit_queue", "status_exchange", statusArgs, headers)
	if err != nil {
		log.Fatalf(err.Error())
	}
}
