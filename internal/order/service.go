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
	publisher *broker.Publisher
}

func NewService(repo *Repository, publisher *broker.Publisher) *Service {
	return &Service{repo: repo, publisher: publisher}
}

func (s *Service) CreateOrder(ctx context.Context, userID string, items []*pb.OrderItem) (*pb.Order, error) {
	log.Printf("Creating order for user %s with %d items", userID, len(items))

	// Toplam fiyatı hesapla.
	var totalPrice float64
	for _, item := range items {
		totalPrice += item.Price * float64(item.Quantity)
	}

	// Siparişi PENDING olarak veritabanına kaydet.
	order, err := s.repo.CreateOrderInTx(ctx, userID, totalPrice, items)
	if err != nil {
		return nil, fmt.Errorf("failed to create order in repository: %w", err)
	}
	log.Printf("Order %s created in DB with PENDING status", order.Id)

	// Başarılı olursa, OrderCreated olayını yayınla.
	event := broker.OrderCreatedEvent{
		OrderID:    order.Id,
		UserID:     order.UserId,
		TotalPrice: order.TotalPrice,
		Items:      order.Items,
	}
	if err := s.publisher.PublishOrderCreated(event); err != nil {
		// KRİTİK: Olay yayınlama başarısız olursa ne yapmalı?
		// Bu durumda siparişi 'FAILED' olarak işaretleyebilir veya telafi işlemi başlatabiliriz.
		// Şimdilik sadece logluyoruz.
		log.Printf("CRITICAL: Failed to publish OrderCreated event for order %s: %v", order.Id, err)
		// return nil, fmt.Errorf("order created but failed to publish event: %w", err)
	}

	return order, nil
}
