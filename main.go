package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	interrupt := make(chan os.Signal)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(interrupt)

	rabbit := NewRabbit()
	if err := rabbit.Connect(); err != nil {
		fmt.Printf("Can't connect to RabbitMQ server, err: %s", err.Error())
	} else {
		fmt.Println("Successfully connected to RabbitMQ server")
	}
	err := rabbit.StartConsumer("test-queue")
	if err != nil {
		fmt.Printf("Error: %s", err.Error())
	}
	select {
	case <-interrupt:
		fmt.Println("Interrupt signal received")
		break
	}
	fmt.Println("Application is about to close")
}
