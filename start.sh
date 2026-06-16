#!/bin/sh

echo "Starting distributed systems demo..."

PORT=8081 ./user-service-bin &

PORT=8082 ./product-service-bin &
PORT=8083 LATENCY_MS=400 ./product-service-bin &
PORT=8085 LATENCY_MS=150 ./product-service-bin &
PORT=8086 ./product-service-bin &

PORT=8084 PRODUCT_SERVICE_URL=http://localhost:8082 ./order-service-bin &

sleep 2

exec ./gateway-bin