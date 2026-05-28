FROM golang:1.22-alpine AS build

WORKDIR /src

RUN apk add --no-cache git

COPY shared/proto ./shared/proto
COPY shared/platform ./shared/platform
COPY server ./server

WORKDIR /src/server

ARG SERVICE_PATH=cmd/order-ingestion-service

RUN go mod download
RUN mkdir -p /out
RUN go build -o /out/service ./${SERVICE_PATH}

FROM alpine:3.20

RUN apk --no-cache add ca-certificates

WORKDIR /app

COPY --from=build /out/service /app/service

EXPOSE 50062 8082

ENTRYPOINT ["/app/service"]
