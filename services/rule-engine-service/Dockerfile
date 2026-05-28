FROM golang:1.22-alpine AS builder

WORKDIR /src

RUN apk add --no-cache git

COPY shared/proto ./shared/proto
COPY shared/platform ./shared/platform
COPY rule-engine-service ./rule-engine-service

WORKDIR /src/rule-engine-service
RUN go mod download
RUN mkdir -p /out
RUN go build -o /out/rule-engine-service .

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /out/rule-engine-service ./rule-engine-service

EXPOSE 50058

CMD ["./rule-engine-service"]
