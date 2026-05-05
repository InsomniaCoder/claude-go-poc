# Makefile
.PHONY: build run down test lint migrate gen

build:
	go build -o bin/server ./cmd/server
	go build -o bin/worker ./cmd/worker

run:
	docker compose up --build

down:
	docker compose down

test:
	go test ./...

lint:
	golangci-lint run

migrate:
	go run ./cmd/migrate

gen:
	go generate ./...
