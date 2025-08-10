package order

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	pb "github.com/ogozo/proto-definitions/gen/go/order"
)

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{db: db}
}

// CreateOrderInTx, siparişi ve kalemlerini tek bir veritabanı işlemi (transaction) içinde oluşturur.
func (r *Repository) CreateOrderInTx(ctx context.Context, userID string, totalPrice float64, items []*pb.OrderItem) (*pb.Order, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	// Eğer fonksiyonda bir hata olursa, transaction'ı geri al (rollback).
	defer tx.Rollback(ctx)

	// Adım 1: Ana sipariş kaydını oluştur.
	orderQuery := `INSERT INTO orders (user_id, total_price, status) VALUES ($1, $2, $3) RETURNING id, status, created_at`
	var newOrder pb.Order
	newOrder.UserId = userID
	newOrder.TotalPrice = totalPrice

	err = tx.QueryRow(ctx, orderQuery, userID, totalPrice, "PENDING").Scan(&newOrder.Id, &newOrder.Status)
	if err != nil {
		return nil, fmt.Errorf("failed to insert order: %w", err)
	}

	// Adım 2: Sipariş kalemlerini (items) oluştur.
	batch := &pgx.Batch{}
	itemQuery := `INSERT INTO order_items (order_id, product_id, quantity, price) VALUES ($1, $2, $3, $4)`
	for _, item := range items {
		batch.Queue(itemQuery, newOrder.Id, item.ProductId, item.Quantity, item.Price)
	}

	br := tx.SendBatch(ctx, batch)
	// Batch'teki tüm sorguları çalıştır.
	if _, err := br.Exec(); err != nil {
		return nil, fmt.Errorf("failed to execute batch insert for order items: %w", err)
	}
	if err := br.Close(); err != nil {
		return nil, fmt.Errorf("failed to close batch insert for order items: %w", err)
	}

	// Adım 3: Her şey başarılıysa, transaction'ı onayla (commit).
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	newOrder.Items = items
	return &newOrder, nil
}
