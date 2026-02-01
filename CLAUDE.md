# CLAUDE.md

## Project Overview

Event-driven microservices PoC: an image analysis pipeline using RabbitMQ for messaging, Neo4j for event lineage, MinIO for storage, and MailHog for email. Seven services process images through fetch → metadata extraction + object detection (parallel) → annotation (fan-in) → storage → notification.

## Tech Stack

- **Node.js services** (workflow-api, image-fetcher, storage-service, notification-service): plain JS, no TypeScript, no build step. Dependencies: amqplib, neo4j-driver, fastify (API only), sharp, minio, nodemailer, axios, uuid.
- **Python services** (metadata-extractor, object-detection, image-annotator): Python 3.11, no frameworks. Dependencies: pika, neo4j, Pillow, exifread, ultralytics (YOLO), opencv-python-headless.
- **Infrastructure**: Docker Compose with RabbitMQ 3.12, Neo4j 5.x, MinIO, MailHog.

## Key Architectural Patterns

- **Single topic exchange**: `imageanalyzer.events` with routing keys matching event types (e.g. `image.fetched`). Each service has its own durable queue.
- **Event envelope**: All messages use `{eventId, eventType, workflowId, timestamp, payload}`.
- **Neo4j lineage**: Every service records `(:Event)-[:BELONGS_TO]->(:Workflow)` and `(:Event)-[:TRIGGERS]->(:Event)` relationships. The Workflow node also stores the email address.
- **Fan-in** (image-annotator): Uses an in-memory dict keyed by workflowId, consuming from two separate queues. Annotates only when both `metadata` and `detections` arrive.
- **Shared volume**: `/data/images` is mounted to all services for image file passing.

## File Conventions

- Node.js services: `package.json`, `index.js`, `Dockerfile` — each in `services/<name>/`
- Python services: `requirements.txt`, `main.py`, `Dockerfile` — each in `services/<name>/`
- No `.env` files; all config via environment variables in `docker-compose.yml`

## Common Patterns When Modifying Services

### Adding a new event type
1. Publisher: construct the event envelope and call `channel.publish(EXCHANGE, '<event.type>', ...)`
2. Consumer: declare a durable queue, bind it to the exchange with the routing key, consume messages
3. Record the event in Neo4j using the shared `recordEvent` pattern (MERGE Workflow, CREATE Event, CREATE BELONGS_TO, optionally CREATE TRIGGERS from the previous event)

### Adding a new service
1. Create `services/<name>/` with the appropriate files (see conventions above)
2. Add the service to `docker-compose.yml` with `RABBITMQ_URL`, `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD` environment variables and the shared volume
3. Add `depends_on` with health check conditions for rabbitmq and neo4j

### Neo4j queries
- Credentials: `neo4j` / `password123`
- URI: `bolt://neo4j:7687` (from containers), `bolt://localhost:7687` (from host)

### RabbitMQ
- Credentials: `admin` / `admin`
- AMQP URL: `amqp://admin:admin@rabbitmq:5672` (from containers)

## Running & Testing

```bash
# Start everything
docker-compose up --build

# Trigger a workflow
curl -X POST http://localhost:3000/workflows \
  -H "Content-Type: application/json" \
  -d '{"imageUrl":"https://upload.wikimedia.org/wikipedia/commons/thumb/3/3a/Cat03.jpg/1200px-Cat03.jpg","email":"demo@example.com"}'

# Check status
curl http://localhost:3000/workflows/<workflowId>
```

Verify via: MailHog (http://localhost:8025), Neo4j Browser (http://localhost:7474), MinIO Console (http://localhost:9001), RabbitMQ Management (http://localhost:15672).

## Gotchas

- The object-detection service downloads the YOLOv8 nano model on first run — the initial build/start is slow.
- The image-annotator fan-in state is in-memory; if the service restarts between receiving the two events for a workflow, that workflow will hang. This is acceptable for a PoC.
- MinIO presigned URLs use the internal hostname (`minio:9000`) — they won't resolve from the host without `/etc/hosts` or a proxy. For local testing, download directly from the MinIO console.
- MailHog has no persistence; emails are lost on restart.
