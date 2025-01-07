package main

import(
	"log"
	amqp "github.com/rabbitmq/amqp091-go"
)


func main(){
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")

	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()
	ch,err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()


	q, err := ch.QueueDeclare(
		"hello", //name
		false, //durable
		false, //delete when unused
		false, //exclusive
		false, //no-wait
		nil, //arguments

	)

	failOnError(err, "Failed to declare a queue")

	msgs,err := ch.Consume(
		q.Name, //queue
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	failOnError(err, "Failed to register a consumer")

	forever := make(chan struct{})

	go func(){
		for d:= range msgs{
			log.Printf("Received a message: %s", d.Body)
		}
	}()


	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever // blocking

}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}