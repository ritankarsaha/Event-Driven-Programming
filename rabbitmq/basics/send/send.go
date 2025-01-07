package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"time"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	// Connect to the RabbitMQ server
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// Declare a queue to send the message to the queue
	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Enter your message (type 'exit' to quit): ")
		body, _ := reader.ReadString('\n')
		body = body[:len(body)-1] 

		if body == "exit" {
			fmt.Println("Exiting...")
			break
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = ch.PublishWithContext(
			ctx,
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			},
		)
		failOnError(err, "Failed to publish a message")

		log.Printf(" [x] Sent: %s\n", body)
	}
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
