package main

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
)

type Config struct {
	Host     string
	Port     int
	Username string
	Password string
	Vhost    string
}

type Rabbit struct {
	config     Config
	connection *amqp.Connection
	lock       sync.Mutex
}

// NewRabbit returns a Rabbit instance.
func NewRabbit() *Rabbit {
	// setup appropriate values
	config := Config{
		Host:     "shostakovich",
		Username: "guest",
		Password: "guest",
		Port:     5672,
		Vhost:    "/",
	}
	return &Rabbit{
		config: config,
	}
}

// Connect connects to RabbitMQ server.
func (r *Rabbit) Connect() error {
	r.lock.Lock()
	defer r.lock.Unlock()
	// Check if connection is already available
	if r.connection == nil || r.connection.IsClosed() {
		// Try connecting
		con, err := amqp.DialConfig(fmt.Sprintf(
			"amqp://%s:%s@%s:%d/%s",
			r.config.Username,
			r.config.Password,
			r.config.Host,
			r.config.Port,
			r.config.Vhost,
		), amqp.Config{})
		if err != nil {
			return err
		}
		r.connection = con
	}

	return nil
}

func (r *Rabbit) StartConsumer(queueName string) error {
	chn, err := r.connection.Channel()
	if err != nil {
		return err
	}

	// Make sure we process 1 message at a time
	if err := chn.Qos(1, 0, false); err != nil {
		return err
	}
	_, err = chn.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		amqp.Table{"x-queue-type": "quorum"}) // This will ensure that the created queue is quorum-queue
	if err != nil {
		fmt.Printf("Error creating queue with name: %s, err: %s", queueName, err.Error())
		return err
	}

	messages, err := chn.Consume(
		queueName,
		queueName+"-consumer",
		false,
		true,
		false,
		false,
		nil,
	)
	if err != nil {
		fmt.Printf("Unable to start consumer for webhook queue: %s, err: %s", queueName, err.Error())
		return err
	}
	go func() {
		// This for-loop will wait indefinitely or until channel is closed
		for msg := range messages {
			fmt.Printf("Message: %v", msg.Body)
			if err = msg.Ack(false); err != nil {
				fmt.Printf("Unable to acknowledge the message, err: %s", err.Error())
			}
		}
	}()
	return nil
}
