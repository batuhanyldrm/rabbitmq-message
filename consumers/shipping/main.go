package main

import (
	"log"

	"github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, _ := amqp091.Dial("amqp://guest:guest@rabbitmq:5672/")
	defer conn.Close()

	ch, _ := conn.Channel()
	defer ch.Close()

	q, _ := ch.QueueDeclare("shipping_queue", true, false, false, false, nil)

	ch.QueueBind(q.Name, "order.shipping.*", "order_exchange", false, nil)

	msgs, _ := ch.Consume(q.Name, "", true, false, false, false, nil)

	log.Println("Shipping Service dinliyor...")

	for msg := range msgs {
		log.Println("[SHIPPING]", string(msg.Body))
	}
}
