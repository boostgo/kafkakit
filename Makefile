.PHONY: all test test-integration lint vet cover tidy

all: vet lint test

## test: unit tests with the race detector (no Kafka required)
test:
	go test -race ./...

## test-integration: also run tests against a real broker
test-integration:
	KAFKA_BROKERS=$${KAFKA_BROKERS:-localhost:9092} go test -race -count=1 ./...

## lint: run golangci-lint
lint:
	golangci-lint run ./...

## vet: run go vet
vet:
	go vet ./...

## cover: tests with a coverage report
cover:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

## tidy: tidy go.mod/go.sum
tidy:
	go mod tidy
