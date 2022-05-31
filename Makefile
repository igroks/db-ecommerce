.PHONY: build
build:
	@docker build -t db-ecommerce .

.PHONY: run
run:
	@docker run -it db-ecommerce
	