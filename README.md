# Order Service

[![Go Version](https://img.shields.io/badge/Go-1.22%2B-blue.svg)](https://golang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

The central microservice for orchestrating the order fulfillment process using the Saga pattern in the E-Commerce Microservice Platform.

## 📖 Overview

This service is the heart of the platform's business logic, responsible for managing the entire lifecycle of a customer order. It solves the critical problem of maintaining data consistency across multiple microservices in a distributed environment. Its core responsibilities include:

*   **Order Creation:** Initiates the order process by creating an order in a `PENDING` state in its PostgreSQL database.
*   **Saga Orchestration:** Kicks off the order saga by publishing an `OrderCreated` event to RabbitMQ.
*   **State Management:** Listens for events from other services (e.g., `StockUpdateResult`) to determine the final state of an order.
*   **Finalization:** Updates the order status to `CONFIRMED` or `CANCELLED` based on the outcome of the saga and publishes the final `OrderConfirmed` event.

## 🚀 Architecture

The Order Service is a prime example of an event-driven microservice. It acts as both a **publisher** and a **consumer** in the RabbitMQ event stream, following a Clean Architecture pattern internally.

```mermaid
graph TD
    subgraph "Request & Initial Event"
        GW[🌐 API Gateway] -- gRPC --> Handler;
        Handler -- Calls --> BusinessService[Service];
        BusinessService -- "Create PENDING Order" --> Repo[Repository];
        Repo -- CRUD --> PG[(🗄️ PostgreSQL)];
        BusinessService -- "Publishes 'OrderCreated'" --> RMQ_Out[("📨 RabbitMQ")];
    end
    
    subgraph "Saga Completion Flow"
        RMQ_In[("📨 RabbitMQ")] -- "StockUpdateResult Event" --> Consumer;
        Consumer -- Calls --> BusinessService;
        BusinessService -- "Update Status (CONFIRMED/CANCELLED)" --> Repo;
        BusinessService -- "Publishes 'OrderConfirmed'" --> RMQ_Out;
    end

    subgraph "Observability"
        Handler -- "Traces/Logs/Metrics" --> OBS[(🔭 Observability Stack)];
        Repo -- "Traces/Logs" --> OBS;
        Consumer -- "Traces/Logs" --> OBS;
    end
```

### Saga Flow Orchestration

1.  The service receives a `CreateOrder` request from the API Gateway.
2.  It creates a new order in its own PostgreSQL database with a `PENDING` status. This is its local transaction.
3.  Upon success, it publishes an `OrderCreated` event.
4.  It then waits and listens for a `StockUpdateResult` event from the Product Service.
5.  If the stock update was successful, it updates the order's status to `CONFIRMED`.
6.  If the stock update failed, it updates the order's status to `CANCELLED` (a compensating transaction).
7.  Finally, upon successful confirmation, it publishes an `OrderConfirmed` event to notify other services (like the Cart Service) to complete the flow.

## 🛠️ Tech Stack

| Category | Technology |
| :--- | :--- |
| **Language** | Go |
| **Communication** | gRPC (Server), RabbitMQ (Consumer/Producer), Protobuf |
| **Database** | PostgreSQL (`pgx/v5` driver) |
| **Observability** | OpenTelemetry (`otelgrpc`, `otelpgx`), Prometheus, Zap |
| **Containerization** | Docker |

## ⚙️ Setup & Running

This service is designed to be run as part of the complete platform using Docker Compose.

### Prerequisites

*   Docker & Docker Compose
*   All project repositories must be cloned at the same directory level.

### Running

The service is automatically built and started when you run the main `docker-compose up` command from the `docker-compose-environment` repository.

For detailed instructions, please see the [main project README](https://github.com/ogozo/docker-compose-environment/blob/main/README.md).

### Environment Variables

The service is configured via the `env/order.env` file in the main compose directory.

| Variable | Description | Example |
| :--- | :--- | :--- |
| `GRPC_PORT` | The port on which the gRPC server listens. | `:50054` |
| `METRICS_PORT`| The port for the `/metrics` HTTP endpoint. | `:9004` |
| `DATABASE_URL`| Connection string for the PostgreSQL database. | `postgres://admin:secret@postgres-db:5432/ecommerce`|
| `RABBITMQ_URL`| Connection string for the RabbitMQ broker. | `amqp://guest:guest@rabbitmq:5672/` |
| `OTEL_SERVICE_NAME`| Service name for observability. | `service-order` |
| `OTEL_EXPORTER_OTLP_ENDPOINT`| OTLP endpoint for Jaeger. | `jaeger-tracer:4317` |

## 📡 gRPC & Event API

This service exposes its functionality via the `OrderService` gRPC interface and interacts with multiple RabbitMQ events.

### gRPC API

*   **`rpc CreateOrder(CreateOrderRequest) returns (CreateOrderResponse)`**: Initiates the saga by creating a `PENDING` order and publishing the initial `OrderCreated` event.
*   **`rpc GetOrder(GetOrderRequest) returns (GetOrderResponse)`**: Retrieves the current status and details of a specific order.

### Asynchronous API (Events)

*   **Publishes:** `OrderCreated` Event
    *   **Trigger:** After a new order is saved in a `PENDING` state.
    *   **Purpose:** To ask the Product Service to reserve stock.
*   **Listens for:** `StockUpdateResult` Event
    *   **Action:** Updates the order status to `CONFIRMED` or `CANCELLED` based on the event's payload.
*   **Publishes:** `OrderConfirmed` Event
    *   **Trigger:** After an order's status is successfully updated to `CONFIRMED`.
    *   **Purpose:** To notify other services (like the Cart Service) that the order is finalized.
