.PHONY: run
run:
	@docker-compose up -d

.PHONY: run-log
run-log:
	@docker-compose up

.PHONY: stop
stop:
	@docker-compose down
