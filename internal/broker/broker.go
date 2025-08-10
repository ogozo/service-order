package broker

import (
	"encoding/json"
	"fmt"
	"log"

	pb "github.com/ogozo/proto-definitions/gen/go/order"
	"github.com/streadway/amqp"
)

// GENEL YAPI
type Broker struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

func NewBroker(amqpURL string) (*Broker, error) {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	return &Broker{conn: conn, channel: channel}, nil
}

func (b *Broker) Close() {
	if b.channel != nil {
		b.channel.Close()
	}
	if b.conn != nil {
		b.conn.Close()
	}
}

// --- PUBLISHER BÖLÜMÜ ---

type OrderCreatedEvent struct {
	OrderID    string          `json:"order_id"`
	UserID     string          `json:"user_id"`
	TotalPrice float64         `json:"total_price"`
	Items      []*pb.OrderItem `json:"items"`
}

func (b *Broker) PublishOrderCreated(event OrderCreatedEvent) error {
	// Gerekli Exchange'i deklare edelim.
	err := b.channel.ExchangeDeclare("orders_exchange", "fanout", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to declare an exchange: %w", err)
	}

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	err = b.channel.Publish("orders_exchange", "", false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	log.Printf("✅ Published OrderCreated event for order %s", event.OrderID)
	return nil
}

// --- CONSUMER BÖLÜMÜ ---

type StockUpdateResultEvent struct {
	OrderID string `json:"order_id"`
	Success bool   `json:"success"`
	Reason  string `json:"reason,omitempty"`
}

func (b *Broker) StartStockUpdateResultConsumer(handler func(event StockUpdateResultEvent)) error {
	err := b.channel.ExchangeDeclare("stock_update_exchange", "fanout", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to declare stock_update_exchange: %w", err)
	}

	q, err := b.channel.QueueDeclare("order_service_stock_update_queue", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to declare a queue: %w", err)
	}

	err = b.channel.QueueBind(q.Name, "", "stock_update_exchange", false, nil)
	if err != nil {
		return fmt.Errorf("failed to bind a queue: %w", err)
	}

	msgs, err := b.channel.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to register a consumer: %w", err)
	}

	go func() {
		for d := range msgs {
			log.Printf("📩 Received StockUpdateResult event: %s", d.Body)
			var event StockUpdateResultEvent
			if err := json.Unmarshal(d.Body, &event); err != nil {
				log.Printf("Error unmarshalling StockUpdateResultEvent: %v", err)
				continue // Bir sonraki mesaja geç
			}
			handler(event) // Olayı işleyecek olan service katmanına gönder
		}
	}()

	log.Println("👂 Listening for StockUpdateResult events...")
	return nil
}
