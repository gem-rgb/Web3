module github.com/example/rms/risk-evaluation-service

go 1.22

require (
	github.com/example/rms/shared/proto v0.0.0
	github.com/segmentio/kafka-go v0.4.47
	google.golang.org/grpc v1.59.0
)

replace github.com/example/rms/shared/proto => ../shared/proto
