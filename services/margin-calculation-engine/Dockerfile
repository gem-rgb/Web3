FROM golang:1.22-alpine AS builder

WORKDIR /src

RUN apk add --no-cache git

COPY shared/proto ./shared/proto
COPY shared/platform ./shared/platform
COPY services/margin-calculation-engine ./services/margin-calculation-engine

WORKDIR /src/services/margin-calculation-engine
RUN go mod download
RUN mkdir -p /out
RUN go build -o /out/margin-calculation-engine .

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /out/margin-calculation-engine ./margin-calculation-engine

EXPOSE 50057

CMD ["./margin-calculation-engine"]
