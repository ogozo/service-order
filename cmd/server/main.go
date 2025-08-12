package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"

	"github.com/jackc/pgx/v4/pgxpool"
	pb "github.com/ogozo/proto-definitions/gen/go/order"
	"github.com/ogozo/service-order/internal/broker"
	"github.com/ogozo/service-order/internal/config"
	"github.com/ogozo/service-order/internal/observability"
	"github.com/ogozo/service-order/internal/order"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	config.LoadConfig()
	cfg := config.AppConfig

	shutdown, err := observability.InitTracerProvider(ctx, cfg.OtelServiceName, cfg.OtelExporterEndpoint)
	if err != nil {
		log.Fatalf("failed to initialize tracer provider: %v", err)
	}
	defer func() {
		if err := shutdown(ctx); err != nil {
			log.Fatalf("failed to shutdown tracer provider: %v", err)
		}
	}()

	br, err := broker.NewBroker(cfg.RabbitMQURL)
	if err != nil {
		log.Fatalf("Failed to create broker: %v", err)
	}
	defer br.Close()
	log.Println("RabbitMQ broker connected.")

	dbpool, err := pgxpool.Connect(context.Background(), cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	defer dbpool.Close()
	log.Println("Database connection successful for order service.")

	orderRepo := order.NewRepository(dbpool)
	orderService := order.NewService(orderRepo, br)
	orderHandler := order.NewHandler(orderService)

	if err := br.StartStockUpdateResultConsumer(orderService.HandleStockUpdateResultEvent); err != nil {
		log.Fatalf("Failed to start stock update result consumer: %v", err)
	}

	lis, err := net.Listen("tcp", cfg.GRPCPort)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", cfg.GRPCPort, err)
	}
	s := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	pb.RegisterOrderServiceServer(s, orderHandler)

	log.Printf("Order gRPC server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
