.PHONY: run
run:
	@docker-compose up 

.PHONY: stop
stop:
	@docker-compose down

.PHONY: access-container
access-container:
	@docker exec -it db-ecommerce_postgres_1 bash

.PHONY: download-amazon-meta
download-amazon-meta:
	@mkdir resources
	@wget --load-cookies /tmp/cookies.txt "https://docs.google.com/uc?export=download&confirm=$(wget --quiet --save-cookies /tmp/cookies.txt --keep-session-cookies --no-check-certificate 'https://docs.google.com/uc?export=download&id=1oP8JbNUIP-F7iW8X1C8eKTRKGWzE3O4p' -O- | sed -rn 's/.*confirm=([0-9A-Za-z_]+).*/\1\n/p')&id=1oP8JbNUIP-F7iW8X1C8eKTRKGWzE3O4p" -O resources/amazon-meta.zip && rm -rf /tmp/cookies.txt
