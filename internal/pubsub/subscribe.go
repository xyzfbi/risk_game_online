package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("could not declare and bind queue: %w", err)
	}
	deliveries, err := ch.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("could not start consuming messages: %w", err)
	}

	go func() {
		defer ch.Close()
		for msg := range deliveries {
			var val T
			err := json.Unmarshal(msg.Body, &val)
			if err != nil {
				fmt.Printf("could not unmarshal message: %v\n", err)
				continue
			}
			handler(val)

			err = msg.Ack(false)
			if err != nil {
				fmt.Printf("could not acknowledge message: %v\n", err)
			}
		}
	}()

	return nil
}
