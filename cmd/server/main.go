package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const rabbitConnString = "amqp://guest:guest@localhost:5672/"

	fmt.Println("Starting Peril server...")

	conn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		fmt.Println("Failed to connect to RabbitMQ:", err)
		return
	}
	defer conn.Close()
	fmt.Println("Connected to RabbitMQ successfully!")

	amqpChannel, err := conn.Channel()
	if err != nil {
		fmt.Println("Failed to open a channel:", err)
		return
	}
	defer amqpChannel.Close()
	fmt.Println("Channel opened successfully!")

	_, queue, err := pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".*",
		pubsub.QueueTypeDurable,
	)
	if err != nil {
		fmt.Println("Failed to declare and bind queue:", err)
		return
	}
	fmt.Printf("Queue %v declared and bound successfully!\n", queue.Name)

	gamelogic.PrintServerHelp()

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		command := input[0]
		
		switch command {
		case "pause":
			fmt.Println("Pausing the game...")
			err = pubsub.PublishJSON(
				amqpChannel,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: true,
				},
			)
			if err != nil {
				fmt.Println("Failed to publish message:", err)
				return
			}
		case "resume":
			fmt.Println("Resuming the game...")
			err = pubsub.PublishJSON(
				amqpChannel,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false,
				},
			)
			if err != nil {
				fmt.Println("Failed to publish message:", err)
				return
			}
		case "quit":
			fmt.Println("Quitting the game...")
			return
		default:
			fmt.Println("Unknown command. Please use 'pause', 'resume', or 'quit'.")
		}
	}
}
