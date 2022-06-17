.PHONY: run
run:
	@docker-compose up 

.PHONY: stop
stop:
	@docker-compose down

.PHONY: access-container
access-container:
	@docker exec -it db-ecommerce_postgres_1 bash
