package main

import(
	"encoding/json"
	"fmt"
	"log"
	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"

)

type Comment struct {

	Text 
}
