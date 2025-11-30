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

	// Queue
	q, _ := ch.QueueDeclare("payment_queue", true, false, false, false, nil)

	// Bind
	ch.QueueBind(q.Name, "order.payment.*", "order_exchange", false, nil)

	msgs, _ := ch.Consume(q.Name, "", true, false, false, false, nil)

	log.Println("Payment Service dinliyor...")

	for msg := range msgs {
		log.Println("[PAYMENT]", string(msg.Body))
	}
}
