package main

import (
	"fmt"
	"log"
	"context"
	"github.com/segmentio/kafka-go"
)

func main() {
    // make a new reader that consumes from topic-A, partition 0, at offset 42
    r := kafka.NewReader(kafka.ReaderConfig{
         Brokers:   []string{"localhost:9092"},
         Topic:     "test_topic",
         Partition: 0,
         MinBytes:  10e3, // 10KB
         MaxBytes:  10e6, // 10MB
    })
    r.SetOffset(23)

    for {
        m, err := r.ReadMessage(context.Background())
        if err != nil {
            break
        }
        fmt.Printf("Message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
    }

    if err := r.Close(); err != nil {
        log.Fatal("failed to close reader:", err)
    }
}


