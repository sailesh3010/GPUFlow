# Build stage
FROM golang:1.27-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /gpuflow ./cmd/gpuflow

# Runtime stage
FROM alpine:3.20

RUN apk --no-cache add ca-certificates
COPY --from=builder /gpuflow /usr/local/bin/gpuflow

EXPOSE 8080 9090

ENTRYPOINT ["gpuflow"]
CMD ["serve"]
