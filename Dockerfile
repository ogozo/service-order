FROM golang:1.24.6-alpine AS build

RUN apk update && apk add --no-cache gcc libc-dev ca-certificates

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -ldflags="-w -s" -o /app/server ./cmd/server

FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /app/server /app/server

ENTRYPOINT ["/app/server"]