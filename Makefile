.PHONY: install install-node install-python test test-node test-python up down logs clean

NODE_SERVICES := workflow-api image-fetcher storage-service notification-service
PYTHON_SERVICES := metadata-extractor object-detection image-annotator

## install: Install dependencies for all services
install: install-node install-python

install-node:
	@for svc in $(NODE_SERVICES); do \
		echo "==> Installing $$svc"; \
		cd services/$$svc && npm install && cd ../..; \
	done

install-python:
	@for svc in $(PYTHON_SERVICES); do \
		echo "==> Installing $$svc"; \
		pip install -r services/$$svc/requirements.txt; \
	done

## test: Run unit tests for all services
test: test-node test-python

test-node:
	@for svc in $(NODE_SERVICES); do \
		echo "==> Testing $$svc"; \
		cd services/$$svc && npm test && cd ../..; \
	done

test-python:
	@for svc in $(PYTHON_SERVICES); do \
		echo "==> Testing $$svc"; \
		python -m pytest services/$$svc/tests/; \
	done

## up: Build and start all containers
up:
	docker compose up --build

## down: Stop and remove all containers
down:
	docker compose down

## logs: Tail logs from all containers
logs:
	docker compose logs -f

## clean: Stop containers and remove volumes
clean:
	docker compose down -v

## trigger: Start a sample workflow (usage: make trigger URL=<image-url> EMAIL=<email>)
URL ?= https://upload.wikimedia.org/wikipedia/commons/thumb/3/3a/Cat03.jpg/1200px-Cat03.jpg
EMAIL ?= demo@example.com
trigger:
	curl -s -X POST http://localhost:3000/workflows \
		-H "Content-Type: application/json" \
		-d '{"imageUrl":"$(URL)","email":"$(EMAIL)"}' | python3 -m json.tool || true
