FROM golang:1.21-alpine AS builder
WORKDIR /app
ARG VERSION=unknown
ARG COMMIT=unknown
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-X main.Version=${VERSION} -X main.GitCommit=${COMMIT}" -o redis-shake ./cmd/redis-shake

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/redis-shake .
COPY shake_sync_env.toml .
COPY shake_scan_env.toml .
COPY entrypoint.sh .

RUN chmod +x entrypoint.sh

ENTRYPOINT ["/bin/sh", "entrypoint.sh"]
