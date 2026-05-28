SHELL := pwsh

.PHONY: help dev-up dev-down fmt lint test proto build terraform-fmt terraform-validate

help:
	@Write-Host "Targets: dev-up dev-down fmt lint test proto build terraform-fmt terraform-validate"

dev-up:
	docker compose up -d --build

dev-down:
	docker compose down -v

fmt:
	go fmt ./...

lint:
	go test ./...

test:
	go test ./...

proto:
	.\scripts\generate-proto.ps1

build:
	docker compose build

terraform-fmt:
	cd infra/terraform; terraform fmt -recursive

terraform-validate:
	cd infra/terraform; terraform init -backend=false; terraform validate

