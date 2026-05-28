package proto

import (
	"encoding/json"

	"google.golang.org/grpc/encoding"
)

// JSONCodec keeps the service examples runnable without generated protobuf code.
// The repo still includes the .proto definitions, but this codec lets the local
// Go services exchange the same message shapes in a lightweight way.
type JSONCodec struct{}

func (JSONCodec) Name() string {
	return "json"
}

func (JSONCodec) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (JSONCodec) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func init() {
	encoding.RegisterCodec(JSONCodec{})
}
