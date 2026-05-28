FROM golang:1.22-alpine AS builder

WORKDIR /src

RUN apk add --no-cache git

COPY shared/proto ./shared/proto
COPY shared/platform ./shared/platform
COPY services/audit-logging-service ./services/audit-logging-service

WORKDIR /src/services/audit-logging-service
RUN go mod download
RUN mkdir -p /out
RUN go build -o /out/audit-logging-service .

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /out/audit-logging-service ./audit-logging-service

EXPOSE 50053

CMD ["./audit-logging-service"]
