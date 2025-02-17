package main

import (
	"log"
	"context"
	"github.com/segmentio/kafka-go"
)

func main() {
w := &kafka.Writer{
	Addr:     kafka.TCP("localhost:9092"),
	Topic:   "test_topic",
	Balancer: &kafka.LeastBytes{},
}

err := w.WriteMessages(context.Background(),
	kafka.Message{
		Key:   []byte("Hello"),
		Value: []byte("Francisco"),
	},
	kafka.Message{
		Key:   []byte("Bedtime"),
		Value: []byte("Leo should be asleep zzz"),
	},
)
if err != nil {
    log.Fatal("failed to write messages:", err)
}

if err := w.Close(); err != nil {
    log.Fatal("failed to close writer:", err)
}
}

