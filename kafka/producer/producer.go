package main

import (
	"encoding/json"
	"fmt"
	"log"
	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"
)

type Comment struct {
	Text string `form:"text" json:"text"`
}

func main() {
	app := fiber.New()
	api := app.Group("/api/v1")
	api.Post("/comment", createComment)
	log.Fatal(app.Listen(":3000"))
}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	producer, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}
	return producer, nil
}

func PushCommentToQueue(topic string, message []byte) error {
	if topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}

	brokersUrl := []string{"localhost:29092"}
	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		return fmt.Errorf("failed to connect to producer: %w", err)
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}

func createComment(c *fiber.Ctx) error {
	cmt := new(Comment)
	if err := c.BodyParser(cmt); err != nil {
		log.Println("Error parsing request body:", err)
		return c.Status(fiber.StatusBadRequest).JSON(&fiber.Map{
			"success": false,
			"message": "Invalid request body",
		})
	}


	cmtInBytes, err := json.Marshal(cmt)
	if err != nil {
		log.Println("Error marshalling comment:", err)
		return c.Status(fiber.StatusInternalServerError).JSON(&fiber.Map{
			"success": false,
			"message": "Failed to process comment",
		})
	}


	topic := "comments-topic"
	if err := PushCommentToQueue(topic, cmtInBytes); err != nil {
		log.Println("Error pushing to Kafka:", err)
		return c.Status(fiber.StatusInternalServerError).JSON(&fiber.Map{
			"success": false,
			"message": "Failed to push comment to queue",
		})
	}

	return c.JSON(&fiber.Map{
		"success": true,
		"message": "Comment created",
		"comment": cmt,
	})
}
