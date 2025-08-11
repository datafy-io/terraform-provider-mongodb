default: fmt install generate

.PHONY: build
build:
	go mod tidy
	go build -v ./...

.PHONY: install
install: build
	go install -v ./...

.PHONY: generate
generate:
	cd tools; go generate ./...

.PHONY: fmt
fmt:
	gofmt -s -w -e .

.PHONY: test
test:
	go test -v -cover -timeout=120s -parallel=10 ./...
