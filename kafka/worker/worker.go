package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {

	topic := "comments-topic"
	worker, err := connectConsumer([]string{"localhost:29092"})
	if err != nil {
		log.Fatalf("Failed to connect consumer: %v\n", err)
	}
	defer func() {
		if err := worker.Close(); err != nil {
			log.Printf("Failed to close consumer: %v\n", err)
		}
	}()

	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Failed to start consuming partition: %v\n", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("Failed to close partition consumer: %v\n", err)
		}
	}()

	fmt.Println("Worker started, waiting for messages...")

	sigChain := make(chan os.Signal, 1)
	signal.Notify(sigChain, syscall.SIGINT, syscall.SIGTERM)

	doneCh := make(chan struct{})

	go func() {
		msgCount := 0
		for {
			select {
			case err := <-consumer.Errors():
				log.Printf("Error while consuming: %v\n", err)
			case msg := <-consumer.Messages():
				log.Printf("Received message: %s\n", string(msg.Value))
				msgCount++
			case <-sigChain:
				fmt.Println("Interrupt detected, shutting down...")
				fmt.Printf("Total messages processed: %d\n", msgCount)
				close(doneCh)
				return
			}
		}
	}()

	<-doneCh
	fmt.Println("Worker shut down gracefully.")
}

func connectConsumer(brokersUrl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(brokersUrl, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}
	return consumer, nil
}
