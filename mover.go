package main

import (
	"flag"
	"log"
	"os"

	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	// Загрузка переменных окружения из .env файла
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file")
	}

	rabbitmqURL := os.Getenv("RABBITMQ_URL")
	if rabbitmqURL == "" {
		log.Fatalf("RABBITMQ_URL is not set in the .env file")
	}

	// Чтение аргументов командной строки для названий очередей
	failedQueue := flag.String("from", "", "The name of the 'from' queue")
	destinationQueue := flag.String("to", "", "The name of the 'to' queue")
	flag.Parse()

	if *failedQueue == "" || *destinationQueue == "" {
		log.Fatalf("Both 'from' and 'to' queue names must be provided")
	}

	// Подключение к RabbitMQ
	conn, err := amqp.Dial(rabbitmqURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %s", err)
	}
	defer conn.Close()

	// Создание канала
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %s", err)
	}
	defer ch.Close()

	// Обработка сообщений
	processMessages(ch, *failedQueue, *destinationQueue)
}

func processMessages(ch *amqp.Channel, fromQueueName, toQueueName string) {
	for {
		msg, ok, err := ch.Get(
			fromQueueName, // очередь
			true,          // auto-ack
		)
		if err != nil {
			log.Fatalf("Failed to get a message: %s", err)
			break
		}
		if !ok {
			log.Println("No more messages in queue, exiting...")
			break
		}
		err = ch.Publish(
			"",          // exchange
			toQueueName, // routing key
			false,       // mandatory
			false,       // immediate
			amqp.Publishing{
				ContentType: msg.ContentType,
				Body:        msg.Body,
			})
		if err != nil {
			log.Printf("Failed to publish a message: %s", err)
		} else {
			log.Printf("Moved message: %s", msg.Body)
		}
	}
}
