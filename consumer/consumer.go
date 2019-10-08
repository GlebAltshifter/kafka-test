package main

import (
	"context"
	"fmt"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

const inputTopic = "Test1"

type data struct {
	Number  int
	Factors []int
}

func main() {

	// подписчик очереди Kafka (consumer)
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   inputTopic,
		// GroupID:   "consumer-group-id-3",
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	defer r.Close()

	for {

		// создайм объект контекста с таймаутом в 1 секунд для чтения сообщений
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// читаем очередное сообщение из очереди
		// поскольку вызов блокирующий - передаём контекст с таймаутом
		m, err := r.ReadMessage(ctx)
		if err != nil {
			fmt.Println(err)
			break
		}
		log.Println("recieved:", string(m.Value))
	}
}
