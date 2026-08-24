package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
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
				msg.Nack(false, false)
				continue
			}
			ackType := handler(val)

			switch ackType {
			case Ack:
				err = msg.Ack(false)
				if err != nil {
					fmt.Printf("could not acknowledge message: %v\n", err)
				} else {
					fmt.Println("Acked message")
				}

			case NackRequeue:
				err = msg.Nack(false, true)
				if err != nil {
					fmt.Printf("could not nack (requeue) message: %v\n", err)
				} else {
					fmt.Println("Nacked message (requeued)")
				}

			case NackDiscard:
				err = msg.Nack(false, false)
				if err != nil {
					fmt.Printf("could not nack (discard) message: %v\n", err)
				} else {
					fmt.Println("Nacked message (discarded)")
				}
			}
		}
	}()

	return nil
}
