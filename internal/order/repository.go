package order

import (
	"context"
	"fmt"

	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pb "github.com/ogozo/proto-definitions/gen/go/order"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{db: db}
}

func (r *Repository) CreateOrderInTx(ctx context.Context, userID string, totalPrice float64, items []*pb.OrderItem) (*pb.Order, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	orderQuery := `INSERT INTO orders (user_id, total_price, status) VALUES ($1, $2, $3) RETURNING id, status, created_at`
	var newOrder pb.Order
	newOrder.UserId = userID
	newOrder.TotalPrice = totalPrice

	var statusFromDB string
	var createdAtFromDB time.Time

	err = tx.QueryRow(ctx, orderQuery, userID, totalPrice, "PENDING").Scan(&newOrder.Id, &statusFromDB, &createdAtFromDB)
	if err != nil {
		return nil, fmt.Errorf("failed to insert order: %w", err)
	}

	newOrder.Status = pb.OrderStatus(pb.OrderStatus_value[statusFromDB])
	newOrder.CreatedAt = timestamppb.New(createdAtFromDB)

	batch := &pgx.Batch{}
	itemQuery := `INSERT INTO order_items (order_id, product_id, quantity, price) VALUES ($1, $2, $3, $4)`
	for _, item := range items {
		batch.Queue(itemQuery, newOrder.Id, item.ProductId, item.Quantity, item.Price)
	}

	br := tx.SendBatch(ctx, batch)
	if _, err := br.Exec(); err != nil {
		return nil, fmt.Errorf("failed to execute batch insert for order items: %w", err)
	}
	if err := br.Close(); err != nil {
		return nil, fmt.Errorf("failed to close batch insert for order items: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	newOrder.Items = items
	return &newOrder, nil
}

func (r *Repository) UpdateOrderStatus(ctx context.Context, orderID string, status string) error {
	query := `UPDATE orders SET status = $1 WHERE id = $2`
	cmdTag, err := r.db.Exec(ctx, query, status, orderID)
	if err != nil {
		return fmt.Errorf("failed to update order status: %w", err)
	}
	if cmdTag.RowsAffected() != 1 {
		return fmt.Errorf("order with id %s not found or status not updated", orderID)
	}
	return nil
}

func (r *Repository) GetOrderByID(ctx context.Context, orderID string) (*pb.Order, error) {
	var order pb.Order
	query := `SELECT id, user_id, status FROM orders WHERE id = $1`
	var statusFromDB string

	err := r.db.QueryRow(ctx, query, orderID).Scan(&order.Id, &order.UserId, &statusFromDB)
	if err != nil {
		return nil, err
	}
	order.Status = pb.OrderStatus(pb.OrderStatus_value[statusFromDB])
	return &order, nil
}
