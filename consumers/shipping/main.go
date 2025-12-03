package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// MAX RETRY SAYISI
const MaxRetry = 3

// Timeout süresi (örn 10 saniye)
const ProcessTimeout = 10 * time.Second

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func connectRabbit() *amqp.Connection {
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	failOnError(err, "RabbitMQ bağlantısı kurulamadı")
	return conn
}

func main() {
	conn := connectRabbit()
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Channel açılamadı")

	// --- 1) SHIPPING QUEUE ---
	args := amqp.Table{
		"x-dead-letter-exchange":    "order_exchange",
		"x-dead-letter-routing-key": "shipping.retry",
	}

	q, err := ch.QueueDeclare(
		"shipping_queue",
		true,
		false,
		false,
		false,
		args,
	)
	failOnError(err, "shipping_queue declare hata")

	// --- 2) DLQ ---
	_, err = ch.QueueDeclare(
		"shipping_queue_dlq",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "shipping_queue_dlq declare hata")

	// --- 3) RETRY QUEUE ---
	retryArgs := amqp.Table{
		"x-dead-letter-exchange":    "order_exchange",
		"x-dead-letter-routing-key": "order.shipping.created",
		"x-message-ttl":             int32(5000), // 5 saniye delay
	}

	_, err = ch.QueueDeclare(
		"shipping_queue_retry",
		true,
		false,
		false,
		false,
		retryArgs,
	)
	failOnError(err, "shipping_queue_retry declare hata")

	// Bind
	err = ch.QueueBind(q.Name, "order.shipping.*", "order_exchange", false, nil)
	failOnError(err, "Queue bind hata")

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	failOnError(err, "Consume hata")

	log.Println("Shipping service dinliyor...")

	for msg := range msgs {
		go handleMessage(ch, msg)
	}

	/*
		for msg := range msgs {
			log.Println("[SHIPPING] Mesaj geldi:", string(msg.Body))

			// TEST AMAÇLI: Hata simülasyonu → NACK gönder
			log.Println("Simüle edilmiş hata → mesaj retry kuyruğuna gidiyor")
			msg.Nack(false, false) // requeue=false → retry kuyruğuna gider

			continue
		}
	*/
}

// --------------------
// MESAJ HANDLER
// --------------------

func handleMessage(ch *amqp.Channel, msg amqp.Delivery) {
	// Retry Count header (yoksa 0)
	retryCount := getRetryCount(msg)

	// Timeout context (örn 10 saniye)
	ctx, cancel := context.WithTimeout(context.Background(), ProcessTimeout)
	defer cancel()

	errChan := make(chan error, 1)

	go func() {
		// burada gerçek iş yapılacak
		log.Printf("[SHIPPING] Mesaj işleniyor... (retry=%d) => %s", retryCount, msg.Body)

		// örnek hata simülasyonu:
		if retryCount < 2 {
			errChan <- ErrProcessFailed
			return
		}

		errChan <- nil
	}()

	select {
	case <-ctx.Done():
		log.Println("Timeout oldu → retry")
		retryMessage(ch, msg, retryCount)
		return

	case err := <-errChan:
		if err != nil {
			log.Println("İşleme hatası → retry")
			retryMessage(ch, msg, retryCount)
			return
		}
	}

	// Başarılı → ACK
	msg.Ack(false)
	log.Println("Mesaj başarıyla işlendi.")
}

// ---------------------------
// RETRY & DLQ
// ---------------------------

var ErrProcessFailed = fmt.Errorf("işleme hatası")

func retryMessage(ch *amqp.Channel, msg amqp.Delivery, retryCount int) {
	if retryCount >= MaxRetry {
		log.Println("Max retry limit → DLQ'ya gönderiliyor")

		ch.Publish(
			"order_exchange",
			"shipping.dlq",
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        msg.Body,
			},
		)

		msg.Ack(false)
		return
	}

	// exponential backoff
	delay := time.Duration(math.Pow(3, float64(retryCount))) * time.Second

	log.Printf("Retry (%d/%d) → %v saniye sonra yeniden denenecek",
		retryCount+1, MaxRetry, delay.Seconds())

	ch.Publish(
		"order_exchange",
		"shipping.retry",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        msg.Body,
			Headers: amqp.Table{
				"x-retry-count": retryCount + 1,
			},
		},
	)

	msg.Ack(false)
}

func getRetryCount(msg amqp.Delivery) int {
	if msg.Headers == nil {
		return 0
	}

	val, ok := msg.Headers["x-retry-count"]
	if !ok {
		return 0
	}

	switch v := val.(type) {
	case int32:
		return int(v)
	case int:
		return v
	case string:
		i, _ := strconv.Atoi(v)
		return i
	default:
		return 0
	}
}
