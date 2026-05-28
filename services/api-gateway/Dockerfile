FROM golang:1.22-alpine AS builder

WORKDIR /src

RUN apk add --no-cache git

COPY shared/proto ./shared/proto
COPY shared/platform ./shared/platform
COPY services/api-gateway ./services/api-gateway

WORKDIR /src/services/api-gateway
RUN go mod download
RUN mkdir -p /out
RUN go build -o /out/api-gateway .

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /out/api-gateway ./api-gateway

EXPOSE 50050
EXPOSE 8080

CMD ["./api-gateway"]
