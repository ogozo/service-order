package order

import (
	"context"

	pb "github.com/ogozo/proto-definitions/gen/go/order"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Handler struct {
	pb.UnimplementedOrderServiceServer
	service *Service
}

func NewHandler(service *Service) *Handler {
	return &Handler{service: service}
}

func (h *Handler) CreateOrder(ctx context.Context, req *pb.CreateOrderRequest) (*pb.CreateOrderResponse, error) {
	order, err := h.service.CreateOrder(ctx, req.UserId, req.Items)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "could not create order: %v", err)
	}

	return &pb.CreateOrderResponse{Order: order}, nil
}
