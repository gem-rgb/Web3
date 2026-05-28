module github.com/example/rms/rule-engine-service

go 1.22

require (
	github.com/example/rms/shared/proto v0.0.0
	google.golang.org/grpc v1.59.0
)

replace github.com/example/rms/shared/proto => ../shared/proto
