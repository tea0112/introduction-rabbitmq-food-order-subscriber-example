package consumers

import (
	amqp "github.com/rabbitmq/amqp091-go"
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
