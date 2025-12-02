package main

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	// RabbitMQ bağlantısı
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		log.Fatalf("RabbitMQ bağlantısı başarısız: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Channel açma hatası: %v", err)
	}
	defer ch.Close()

	// Queue oluştur
	q, err := ch.QueueDeclare(
		"shipping_queue",
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		log.Fatalf("QueueDeclare hatası: %v", err)
	}

	// Queue'yu exchange ile bağla
	err = ch.QueueBind(q.Name, "order.shipping.*", "order_exchange", false, nil)
	if err != nil {
		log.Fatalf("QueueBind hatası: %v", err)
	}

	// Mesajları dinle
	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Consume hatası: %v", err)
	}

	log.Println("Shipping Service dinliyor...")

	for msg := range msgs {
		log.Println("[SHIPPING]", string(msg.Body))

		// Manuel ACK eğer yapmak istemezsen aşağıdaki kodu sil consume içindeki ilk false ı true yap
		msg.Ack(false)
	}
}
