FROM golang:1.22-alpine AS builder

WORKDIR /src

RUN apk add --no-cache git

COPY shared/proto ./shared/proto
COPY shared/platform ./shared/platform
COPY services/market-data-consumer ./services/market-data-consumer

WORKDIR /src/services/market-data-consumer
RUN go mod download
RUN mkdir -p /out
RUN go build -o /out/market-data-consumer .

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /out/market-data-consumer ./market-data-consumer

EXPOSE 50054

CMD ["./market-data-consumer"]
