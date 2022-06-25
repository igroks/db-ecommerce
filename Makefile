run-all: download-amazon-meta build run populate-database start-dashboard stop	

.PHONY: download-amazon-meta
download-amazon-meta:
	@sh scripts/download-amazon-meta.sh

.PHONY: build
build:
	@docker-compose build

.PHONY: run
run:
	@docker-compose up -d

.PHONY: populate-database
populate-database:
	@docker exec python python3 src/tp1_3.2.py

.PHONY: start-dashboard
start-dashboard:
	@docker exec -it python python3 src/tp1_3.3.py

.PHONY: stop
stop:
	@docker-compose down

.PHONY: access-postgres
access-postgres:
	@docker exec -it postgres psql -U postgres

.PHONY: access-python
access-python:
	@docker exec -it python bash 

