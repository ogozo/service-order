package broker

import (
	"context"
	"encoding/json"
	"fmt"

	pb "github.com/ogozo/proto-definitions/gen/go/order"
	"github.com/ogozo/service-order/internal/logging"
	"github.com/streadway/amqp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type TraceCarrier map[string]interface{}

func (c TraceCarrier) Get(key string) string {
	if val, ok := c[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

func (c TraceCarrier) Set(key, val string) {
	c[key] = val
}

func (c TraceCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

type Broker struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	tracer  trace.Tracer
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
	tracer := otel.Tracer("service-order.broker")
	return &Broker{conn: conn, channel: channel, tracer: tracer}, nil
}

func (b *Broker) Close() {
	if b.channel != nil {
		b.channel.Close()
	}
	if b.conn != nil {
		b.conn.Close()
	}
}

type OrderCreatedEvent struct {
	OrderID    string          `json:"order_id"`
	UserID     string          `json:"user_id"`
	TotalPrice float64         `json:"total_price"`
	Items      []*pb.OrderItem `json:"items"`
}

func (b *Broker) PublishOrderCreated(ctx context.Context, event OrderCreatedEvent) error {
	exchangeName := "orders_exchange"
	err := b.channel.ExchangeDeclare(exchangeName, "fanout", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to declare an exchange: %w", err)
	}

	spanCtx, span := b.tracer.Start(ctx, exchangeName+" publish", trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemRabbitmq,
			semconv.MessagingDestinationName(exchangeName),
		),
	)
	defer span.End()

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	headers := make(TraceCarrier)
	otel.GetTextMapPropagator().Inject(spanCtx, headers)

	err = b.channel.Publish(exchangeName, "", false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
		Headers:     amqp.Table(headers),
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to publish message")
		return fmt.Errorf("failed to publish message: %w", err)
	}

	logging.Info(spanCtx, "published OrderCreated event", zap.String("order_id", event.OrderID))
	return nil
}

type OrderConfirmedEvent struct {
	OrderID string `json:"order_id"`
	UserID  string `json:"user_id"`
}

func (b *Broker) PublishOrderConfirmed(ctx context.Context, event OrderConfirmedEvent) error {
	exchangeName := "order_confirmed_exchange"
	err := b.channel.ExchangeDeclare(exchangeName, "fanout", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to declare order_confirmed_exchange: %w", err)
	}

	spanCtx, span := b.tracer.Start(ctx, exchangeName+" publish", trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemRabbitmq,
			semconv.MessagingDestinationName(exchangeName),
		),
	)
	defer span.End()

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	headers := make(TraceCarrier)
	otel.GetTextMapPropagator().Inject(spanCtx, headers)

	err = b.channel.Publish(exchangeName, "", false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
		Headers:     amqp.Table(headers),
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to publish message")
		return fmt.Errorf("failed to publish message: %w", err)
	}

	logging.Info(spanCtx, "published OrderConfirmed event", zap.String("order_id", event.OrderID))
	return nil
}

type StockUpdateResultEvent struct {
	OrderID string `json:"order_id"`
	Success bool   `json:"success"`
	Reason  string `json:"reason,omitempty"`
}

func (b *Broker) StartStockUpdateResultConsumer(handler func(ctx context.Context, event StockUpdateResultEvent)) error {
	exchangeName := "stock_update_exchange"
	err := b.channel.ExchangeDeclare(exchangeName, "fanout", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to declare stock_update_exchange: %w", err)
	}

	q, err := b.channel.QueueDeclare("order_service_stock_update_queue", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to declare a queue: %w", err)
	}

	err = b.channel.QueueBind(q.Name, "", exchangeName, false, nil)
	if err != nil {
		return fmt.Errorf("failed to bind a queue: %w", err)
	}

	msgs, err := b.channel.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to register a consumer: %w", err)
	}

	go func() {
		for d := range msgs {
			carrier := make(TraceCarrier)
			for k, v := range d.Headers {
				carrier[k] = v
			}
			parentCtx := otel.GetTextMapPropagator().Extract(context.Background(), carrier)

			spanCtx, span := b.tracer.Start(parentCtx, exchangeName+" receive", trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					semconv.MessagingSystemRabbitmq,
					semconv.MessagingDestinationName(exchangeName),
				),
			)

			logging.Info(spanCtx, "received StockUpdateResult event", zap.String("body", string(d.Body)))
			var event StockUpdateResultEvent
			if err := json.Unmarshal(d.Body, &event); err != nil {
				logging.Error(spanCtx, "failed to unmarshal StockUpdateResultEvent", err)
				span.RecordError(err)
				span.SetStatus(codes.Error, "failed to unmarshal message")
				span.End()
				continue
			}

			handler(spanCtx, event)
			span.End()
		}
	}()

	logging.Info(context.Background(), "listening for StockUpdateResult events")
	return nil
}
