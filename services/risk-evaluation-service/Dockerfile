FROM golang:1.22-alpine AS builder

WORKDIR /src

RUN apk add --no-cache git

COPY shared/proto ./shared/proto
COPY shared/platform ./shared/platform
COPY risk-evaluation-service ./risk-evaluation-service

WORKDIR /src/risk-evaluation-service
RUN go mod download
RUN mkdir -p /out
RUN go build -o /out/risk-evaluation-service .

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /out/risk-evaluation-service ./risk-evaluation-service

EXPOSE 50051

CMD ["./risk-evaluation-service"]
