package  main

import(
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"github.com/IBM/sarama"
)

func main(){

	topic := "comments"
	worker, err := connectConsumer([]string{"localhost:29092"})
	if err != nil {
		panic(err)
	}

	consumer,err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil{
		panic(err)
	}


	fmt.Println("Worker started")
	sigChain := make(chan os.Signal, 1)
	signal.Notify(sigChain, syscall.SIGINT, syscall.SIGTERM)
	msgCount := 0

	doneCh := make(chan struct{})
	go func(){
		for{
			select{
				case err := <-consumer.Errors():
					fmt.Println(err)
				case msg := <-consumer.Messages():
					fmt.Printf("Received message: %s\n", string(msg.Value))
					msgCount++
				case <-sigChain:
					fmt.Println("Interrupt is detected")
					doneCh <- struct{}{}

			}
		}
	}()

	<-doneCh
	fmt.Printf("Processed %d messages\n", msgCount)
	if err := worker.Close(); err != nil{
		fmt.Println(err)
	}


}


func connectConsumer(brokersUrl []string) (sarama.Consumer, error){
	config:= sarama.NewConfig()
	config.Consumer.Return.Errors = true
	conn,err := sarama.NewConsumer(brokersUrl, config)
	if err != nil{
		return nil,err
	}

	return conn,nil
}
