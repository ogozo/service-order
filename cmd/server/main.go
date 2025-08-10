package main

import (
	"context"
	"log"
	"net"

	"github.com/jackc/pgx/v4/pgxpool"
	pb "github.com/ogozo/proto-definitions/gen/go/order"
	"github.com/ogozo/service-order/internal/broker"
	"github.com/ogozo/service-order/internal/config"
	"github.com/ogozo/service-order/internal/order"
	"google.golang.org/grpc"
)

func main() {
	config.LoadConfig()
	cfg := config.AppConfig

	// RabbitMQ Publisher'ı başlat
	publisher, err := broker.NewPublisher(cfg.RabbitMQURL)
	if err != nil {
		log.Fatalf("Failed to create publisher: %v", err)
	}
	defer publisher.Close()
	log.Println("RabbitMQ publisher connected.")

	// Veritabanı bağlantısı
	dbpool, err := pgxpool.Connect(context.Background(), cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	defer dbpool.Close()
	log.Println("Database connection successful for order service.")

	// Bağımlılıkları enjekte et
	orderRepo := order.NewRepository(dbpool)
	orderService := order.NewService(orderRepo, publisher)
	orderHandler := order.NewHandler(orderService)

	// gRPC sunucusunu başlat
	lis, err := net.Listen("tcp", cfg.GRPCPort)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", cfg.GRPCPort, err)
	}
	s := grpc.NewServer()
	pb.RegisterOrderServiceServer(s, orderHandler)

	log.Printf("Order gRPC server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
