FROM golang:1.22-alpine AS build
WORKDIR /src

COPY shared/platform ./shared/platform
COPY services/auth-service ./services/auth-service

WORKDIR /src/services/auth-service
RUN go mod download
RUN mkdir -p /out
RUN go build -o /out/auth-service .

FROM alpine:3.20
RUN addgroup -S rms && adduser -S rms -G rms
USER rms
WORKDIR /app
COPY --from=build /out/auth-service /app/auth-service
EXPOSE 50061 8081
ENTRYPOINT ["/app/auth-service"]
