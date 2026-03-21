all:
	docker compose stop
	docker compose down
	docker compose build
	docker compose up

test:
	go test -tags=integration ./... -cover
#	go test ./...
#	go test -tags=integration ./internal/server
