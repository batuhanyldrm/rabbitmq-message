package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/rabbitmq/amqp091-go"
)

type OrderMessage struct {
	OrderID   string  `json:"order_id"`
	Amount    float64 `json:"amount"`
	CreatedAt string  `json:"created_at"`
}

func main() {
	app := fiber.New()

	// RabbitMQ Bağlantısı
	conn, err := amqp091.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		log.Fatal("RabbitMQ bağlantısı olmadı:", err)
	}
	ch, _ := conn.Channel()

	// Exchange oluştur
	ch.ExchangeDeclare(
		"order_exchange",
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)

	// POST /publish
	app.Post("/publish/:key", func(c *fiber.Ctx) error {
		routingKey := c.Params("key")

		msg := OrderMessage{
			OrderID:   "ORD-" + time.Now().Format("150405"),
			Amount:    149.99,
			CreatedAt: time.Now().Format(time.RFC3339),
		}

		body, _ := json.Marshal(msg)

		err := ch.Publish(
			"order_exchange",
			routingKey,
			false,
			false,
			amqp091.Publishing{
				ContentType: "application/json",
				Body:        body,
			},
		)

		if err != nil {
			return c.Status(500).SendString("Mesaj gönderilemedi!")
		}

		return c.JSON(fiber.Map{
			"status":  "ok",
			"message": string(body),
			"key":     routingKey,
		})
	})

	app.Listen(":8080")
}
