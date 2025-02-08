# Build stage
FROM golang:1.13-alpine AS builder

# Install build dependencies
RUN apk add --no-cache make git

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
COPY vendor/ vendor/

# Copy source code
COPY . .

# Build for linux/amd64
ENV GOOS=linux
ENV GOARCH=amd64
ENV CGO_ENABLED=0
ENV GO111MODULE=on
ENV GOFLAGS=-mod=vendor

# Build all components
RUN make build

# Create a minimal runtime image
FROM alpine:latest

WORKDIR /app

# Copy binaries from builder
COPY --from=builder /app/xtsdb-ingester .
COPY --from=builder /app/xtsdb-flusher .
COPY --from=builder /app/xtsdb-querier .

# Add any required runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Create a non-root user
RUN adduser -D -g '' xtsdb
USER xtsdb

# The binaries will be available in the /app directory
