FROM golang:1.22-alpine AS builder

WORKDIR /src

RUN apk add --no-cache git

COPY shared/proto ./shared/proto
COPY shared/platform ./shared/platform
COPY position-tracking-service ./position-tracking-service

WORKDIR /src/position-tracking-service
RUN go mod download
RUN mkdir -p /out
RUN go build -o /out/position-tracking-service .

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /out/position-tracking-service ./position-tracking-service

EXPOSE 50056

CMD ["./position-tracking-service"]
