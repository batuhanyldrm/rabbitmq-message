package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

type OrderMessage struct {
	OrderID   string  `json:"order_id"`
	Amount    float64 `json:"amount"`
	CreatedAt string  `json:"created_at"`
}

func main() {
	app := fiber.New()

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

	// Exchange oluştur
	err = ch.ExchangeDeclare(
		"order_exchange",
		"topic",
		true,  // durable
		false, // auto-delete
		false, // internal
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		log.Fatalf("ExchangeDeclare hatası: %v", err)
	}

	// POST /publish/:key
	app.Post("/publish/:key", func(c *fiber.Ctx) error {
		routingKey := c.Params("key")

		msg := OrderMessage{
			OrderID:   "ORD-" + time.Now().Format("150405"),
			Amount:    149.99,
			CreatedAt: time.Now().Format(time.RFC3339),
		}

		body, err := json.Marshal(msg)
		if err != nil {
			return c.Status(500).SendString("JSON oluşturulamadı")
		}

		err = ch.Publish(
			"order_exchange",
			routingKey,
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        body,
			},
		)
		if err != nil {
			return c.Status(500).SendString("Mesaj gönderilemedi: " + err.Error())
		}

		return c.JSON(fiber.Map{
			"status":  "ok",
			"message": string(body),
			"key":     routingKey,
		})
	})

	log.Println("API listening on :8080")
	app.Listen(":8080")
}
