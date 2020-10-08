package grpc

//go:generate protoc -I./internal/errors -I. --go_out=paths=source_relative:./internal/errors internal/errors/client_errors.proto
