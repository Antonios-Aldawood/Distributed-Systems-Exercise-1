FROM golang:1.24 AS builder

WORKDIR /app

COPY . .

RUN cd user-service && go build -o /build/user-service-bin .
RUN cd product-service && go build -o /build/product-service-bin .
RUN cd order-service && go build -o /build/order-service-bin .
RUN cd gateway && go build -o /build/gateway-bin .

FROM debian:bookworm-slim

WORKDIR /app

COPY --from=builder /build/ .

COPY user-service/users.db ./users.db
COPY product-service/products.db ./products.db
COPY order-service/orders.db ./orders.db

COPY start.sh .

RUN chmod +x start.sh

EXPOSE 8080

CMD ["./start.sh"]