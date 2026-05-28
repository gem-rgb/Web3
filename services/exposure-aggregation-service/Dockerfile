FROM golang:1.22-alpine AS builder

WORKDIR /src

RUN apk add --no-cache git

COPY shared/proto ./shared/proto
COPY shared/platform ./shared/platform
COPY exposure-aggregation-service ./exposure-aggregation-service

WORKDIR /src/exposure-aggregation-service
RUN go mod download
RUN mkdir -p /out
RUN go build -o /out/exposure-aggregation-service .

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /out/exposure-aggregation-service ./exposure-aggregation-service

EXPOSE 50055

CMD ["./exposure-aggregation-service"]
