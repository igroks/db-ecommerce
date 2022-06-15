.PHONY: build
build:
	@docker build -t db-ecommerce .

.PHONY: run
run:
	@docker run -v ${PWD}:/app -it db-ecommerce
	