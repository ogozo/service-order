package order

import (
	"context"
	"fmt"
	"log"

	pb "github.com/ogozo/proto-definitions/gen/go/order"
	"github.com/ogozo/service-order/internal/broker"
)

type Service struct {
	repo      *Repository
	publisher *broker.Broker
}

func NewService(repo *Repository, broker *broker.Broker) *Service {
	return &Service{repo: repo, publisher: broker}
}

func (s *Service) CreateOrder(ctx context.Context, userID string, items []*pb.OrderItem) (*pb.Order, error) {
	log.Printf("Creating order for user %s with %d items", userID, len(items))

	var totalPrice float64
	for _, item := range items {
		totalPrice += item.Price * float64(item.Quantity)
	}

	order, err := s.repo.CreateOrderInTx(ctx, userID, totalPrice, items)
	if err != nil {
		return nil, fmt.Errorf("failed to create order in repository: %w", err)
	}
	log.Printf("Order %s created in DB with PENDING status", order.Id)

	event := broker.OrderCreatedEvent{
		OrderID:    order.Id,
		UserID:     order.UserId,
		TotalPrice: order.TotalPrice,
		Items:      order.Items,
	}
	if err := s.publisher.PublishOrderCreated(event); err != nil {
		log.Printf("CRITICAL: Failed to publish OrderCreated event for order %s: %v", order.Id, err)
	}

	return order, nil
}

func (s *Service) HandleStockUpdateResultEvent(event broker.StockUpdateResultEvent) {
	var newStatus string
	if event.Success {
		newStatus = "CONFIRMED"
		log.Printf("✅ Order %s CONFIRMED.", event.OrderID)
	} else {
		newStatus = "CANCELLED"
		log.Printf("❌ Order %s CANCELLED due to: %s", event.OrderID, event.Reason)
	}

	err := s.repo.UpdateOrderStatus(context.Background(), event.OrderID, newStatus)
	if err != nil {
		log.Printf("CRITICAL: Failed to update order status for order %s to %s. Error: %v", event.OrderID, newStatus, err)
	}
}
