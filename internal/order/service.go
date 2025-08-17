package order

import (
	"context"
	"fmt"

	pb "github.com/ogozo/proto-definitions/gen/go/order"
	"github.com/ogozo/service-order/internal/broker"
	"github.com/ogozo/service-order/internal/logging"
	"go.uber.org/zap"
)

type Service struct {
	repo      *Repository
	publisher *broker.Broker
}

func NewService(repo *Repository, broker *broker.Broker) *Service {
	return &Service{repo: repo, publisher: broker}
}

func (s *Service) CreateOrder(ctx context.Context, userID string, items []*pb.OrderItem) (*pb.Order, error) {
	logging.Info(ctx, "creating order", zap.String("user_id", userID), zap.Int("item_count", len(items)))

	var totalPrice float64
	for _, item := range items {
		totalPrice += item.Price * float64(item.Quantity)
	}

	order, err := s.repo.CreateOrderInTx(ctx, userID, totalPrice, items)
	if err != nil {
		return nil, fmt.Errorf("failed to create order in repository: %w", err)
	}
	logging.Info(ctx, "order created in DB with PENDING status", zap.String("order_id", order.Id))

	event := broker.OrderCreatedEvent{
		OrderID:    order.Id,
		UserID:     order.UserId,
		TotalPrice: order.TotalPrice,
		Items:      order.Items,
	}
	if err := s.publisher.PublishOrderCreated(ctx, event); err != nil {
		logging.Error(ctx, "CRITICAL: failed to publish OrderCreated event", err, zap.String("order_id", order.Id))
	}

	return order, nil
}

func (s *Service) HandleStockUpdateResultEvent(ctx context.Context, event broker.StockUpdateResultEvent) {
	var newStatus string
	if event.Success {
		newStatus = "CONFIRMED"
		logging.Info(ctx, "order CONFIRMED", zap.String("order_id", event.OrderID))
	} else {
		newStatus = "CANCELLED"
		logging.Info(ctx, "order CANCELLED", zap.String("order_id", event.OrderID), zap.String("reason", event.Reason))
	}

	err := s.repo.UpdateOrderStatus(ctx, event.OrderID, newStatus)
	if err != nil {
		logging.Error(ctx, "CRITICAL: failed to update order status", err,
			zap.String("order_id", event.OrderID),
			zap.String("new_status", newStatus),
		)
		return
	}

	if event.Success {
		order, err := s.repo.GetOrderByID(ctx, event.OrderID)
		if err != nil {
			logging.Error(ctx, "CRITICAL: could not get order details to publish OrderConfirmed event", err, zap.String("order_id", event.OrderID))
			return
		}

		confirmedEvent := broker.OrderConfirmedEvent{
			OrderID: order.Id,
			UserID:  order.UserId,
		}
		if err := s.publisher.PublishOrderConfirmed(ctx, confirmedEvent); err != nil {
			logging.Error(ctx, "CRITICAL: failed to publish OrderConfirmed event", err, zap.String("order_id", event.OrderID))
		}
	}
}
