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
	countMessage := flag.Int("count", 0, "Number of messages to move (0 for all messages)")

	flag.Parse()

	if *failedQueue == "" {
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
	processMessages(ch, *failedQueue, *destinationQueue, *countMessage)
}

func processMessages(ch *amqp.Channel, fromQueueName, toQueueName string, countMessage int) {
	messagesMoved := 0
	for countMessage == 0 || messagesMoved < countMessage {
		msg, ok, err := ch.Get(
			fromQueueName, // очередь
			false,         // auto-ack
		)
		if err != nil {
			log.Fatalf("Failed to get a message: %s", err)
			break
		}
		if !ok {
			log.Println("No more messages in queue, exiting...")
			break
		}

		destinationQueue := toQueueName
		if destinationQueue == "" {
			if enqueueTopic, ok := msg.Headers["enqueue.topic"].(string); ok {
				destinationQueue = enqueueTopic
			} else {
				log.Printf("No 'to' queue specified and no 'enqueue.topic' header found, skipping message")
				msg.Nack(false, true) // Отклоняем сообщение и возвращаем его в очередь
				continue
			}
		}

		err = ch.Publish(
			"",               // exchange
			destinationQueue, // routing key
			false,            // mandatory
			false,            // immediate
			amqp.Publishing{
				ContentType: msg.ContentType,
				Body:        msg.Body,
				Headers:     msg.Headers,
			})
		if err != nil {
			log.Printf("Failed to publish a message: %s", err)
			msg.Nack(false, true) // Отклоняем сообщение и возвращаем его в очередь
		} else {
			log.Printf("Moved message to queue %s: %s", destinationQueue, msg.Body)
			msg.Ack(false) // Подтверждаем обработку сообщения
			messagesMoved++
		}
	}
	log.Printf("Total messages moved: %d", messagesMoved)
}
