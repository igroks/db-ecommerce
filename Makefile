run-all: download-amazon-meta build run insert-database run-dashboard end	

.PHONY: download-amazon-meta
download-amazon-meta:
	@sh download-amazon-meta.sh

.PHONY: build
build:
	@docker-compose build

.PHONY: run
run:
	@docker-compose up -d

.PHONY: insert-database
insert-database:
	@docker exec tp1_aldemir_erlon_glenn_python python3 src/tp1_3.2.py

.PHONY: run-dashboard
run-dashboard:
	@docker exec -it tp1_aldemir_erlon_glenn_python python3 src/tp1_3.3.py

.PHONY: end
end:
	@docker-compose down

.PHONY: access-postgres
access-postgres:
	@docker exec -it tp1_aldemir_erlon_glenn_postgres psql -U postgres

.PHONY: access-python
access-python:
	@docker exec -it tp1_aldemir_erlon_glenn_python bash 

