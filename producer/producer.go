package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	kafka "github.com/segmentio/kafka-go"
)

const topic = "Test1"

func main() {

	// клиент очереди
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, 0)
	if err != nil {
		log.Fatal(err)
	}
	reader := bufio.NewReader(os.Stdin)
	defer conn.Close()

	for {

		// получаем строчку на вход
		fmt.Print("Enter message: ")
		line, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}
		line = strings.TrimSpace(line)

		// пишем в очередь
		_, err = conn.WriteMessages(
			kafka.Message{Value: []byte(line)},
		)
		if err != nil {
			log.Fatal(err)
		}

		// fmt.Println("Written to topic:", line)

	}

}
