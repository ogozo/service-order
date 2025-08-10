package broker

import (
	"encoding/json"
	"fmt"
	"log"

	pb "github.com/ogozo/proto-definitions/gen/go/order"
	"github.com/streadway/amqp"
)

type Publisher struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

func NewPublisher(amqpURL string) (*Publisher, error) {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	// Sipariş olayları için bir "fanout" exchange tanımlıyoruz.
	// Bu exchange'e gelen mesajlar, ona bağlı tüm kuyruklara kopyalanır.
	err = channel.ExchangeDeclare(
		"orders_exchange", // name
		"fanout",          // type
		true,              // durable
		false,             // auto-deleted
		false,             // internal
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare an exchange: %w", err)
	}

	return &Publisher{conn: conn, channel: channel}, nil
}

// OrderCreatedEvent, diğer servislerin anlayacağı olay yapısıdır.
type OrderCreatedEvent struct {
	OrderID    string          `json:"order_id"`
	UserID     string          `json:"user_id"`
	TotalPrice float64         `json:"total_price"`
	Items      []*pb.OrderItem `json:"items"`
}

func (p *Publisher) PublishOrderCreated(event OrderCreatedEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	err = p.channel.Publish(
		"orders_exchange", // exchange
		"",                // routing key (fanout için boş)
		false,             // mandatory
		false,             // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	log.Printf("✅ Published OrderCreated event for order %s", event.OrderID)
	return nil
}

func (p *Publisher) Close() {
	p.channel.Close()
	p.conn.Close()
}
