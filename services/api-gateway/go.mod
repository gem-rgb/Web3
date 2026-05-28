module github.com/example/rms/api-gateway

go 1.22

require (
	github.com/example/rms/shared/proto v0.0.0
	github.com/example/rms/shared/platform v0.0.0
	google.golang.org/grpc v1.59.0
)

replace github.com/example/rms/shared/proto => ../shared/proto
replace github.com/example/rms/shared/platform => ../shared/platform
