FROM golang:1.22-alpine AS builder

WORKDIR /src

RUN apk add --no-cache git

COPY shared/proto ./shared/proto
COPY shared/platform ./shared/platform
COPY services/alerting-service ./services/alerting-service

WORKDIR /src/services/alerting-service
RUN go mod download
RUN mkdir -p /out
RUN go build -o /out/alerting-service .

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /out/alerting-service ./alerting-service

EXPOSE 50052

CMD ["./alerting-service"]
