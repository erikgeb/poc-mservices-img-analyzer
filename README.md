# Event-Driven Image Analyzer PoC

A proof-of-concept microservices architecture that processes images through an event-driven pipeline using RabbitMQ. Given an image URL, the system fetches, normalizes, extracts metadata, detects objects (YOLOv8), annotates, stores, and sends an email notification with a download link.

## Architecture

```
POST /workflows
       │
       ▼
┌────────────────┐ workflow.started ┌────────────────┐ image.fetched ┌──────────────────────┐
│  Workflow API  │────────────────▶ │  Image Fetcher │──────┬───────▶│ Metadata Extractor   │
│   (Node.js)    │                  │    (Node.js)   │      │        │       (Python)       │
└────────────────┘                  └────────────────┘      │        └──────────┬───────────┘
                                                            │                   │
                                                            │                   │ metadata_extracted
                                                            │                   ▼
                                                            │        ┌──────────────────────┐
                                                            └───────▶│  Object Detection    │
                                                                     │   (Python/YOLOv8)    │
                                                                     └──────────┬───────────┘
                                                                                │ objects_detected
                                                                                ▼
                                                                     ┌──────────────────────┐
                                                                     │  Image Annotator     │◀── fan-in
                                                                     │       (Python)       │
                                                                     └──────────┬───────────┘
                                                                                │ image.annotated
                                                                                ▼
                                                                     ┌──────────────────────┐
                                                                     │  Storage Service     │
                                                                     │  (Node.js / MinIO)   │
                                                                     └──────────┬───────────┘
                                                                                │ image.stored
                                                                                ▼
                                                                     ┌──────────────────────┐
                                                                     │ Notification Service │
                                                                     │ (Node.js / MailHog)  │
                                                                     └──────────────────────┘
```

## Services

| Service | Language | Listens on | Emits | Purpose |
|---|---|---|---|---|
| **workflow-api** | Node.js (Fastify) | HTTP | `workflow.started` | REST entry point, validates image URL |
| **image-fetcher** | Node.js | `workflow.started` | `image.fetched` | Downloads & normalizes image (≤1920px JPEG) |
| **metadata-extractor** | Python | `image.fetched` | `image.metadata_extracted` | Extracts EXIF data |
| **object-detection** | Python | `image.fetched` | `image.objects_detected` | YOLOv8 nano object detection (CPU) |
| **image-annotator** | Python | `image.metadata_extracted` + `image.objects_detected` | `image.annotated` | Fan-in: draws bounding boxes + EXIF overlay |
| **storage-service** | Node.js | `image.annotated` | `image.stored` | Uploads to MinIO, generates presigned URL |
| **notification-service** | Node.js | `image.stored` | `notification.sent` | Sends email via MailHog with download link |

## Infrastructure

- **RabbitMQ** 4.2 — message broker (topic exchange `imageanalyzer.events`)
- **Neo4j** 5.x — event lineage and workflow tracking
- **MinIO** — S3-compatible object storage for annotated images
- **MailHog** — local SMTP server for email testing

## Getting Started

### Prerequisites

- Docker and Docker Compose

### Run

```bash
docker compose up --build
```

If you see a warning about buildx/Bake not being installed, disable it:

```bash
# One-off
COMPOSE_BAKE=false docker compose up --build

# Or permanently
docker config set composeUseBuildxBake false
```

Wait for all health checks to pass (RabbitMQ and Neo4j take the longest).

### Trigger a workflow

```bash
curl -X POST http://localhost:3000/workflows \
  -H "Content-Type: application/json" \
  -d '{"imageUrl":"https://upload.wikimedia.org/wikipedia/commons/thumb/3/3a/Cat03.jpg/1200px-Cat03.jpg","email":"example@demo.com"}'
```

### Check workflow status

```bash
curl http://localhost:3000/workflows/<workflowId>
```

### Web UIs

| UI | URL | Credentials |
|---|---|---|
| RabbitMQ Management | http://localhost:15672 | admin / admin |
| Neo4j Browser | http://localhost:7474 | neo4j / password123 |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| MailHog | http://localhost:8025 | — |

### Verify the pipeline

1. Check **MailHog** at http://localhost:8025 for the notification email with the download link
2. Query **Neo4j** for the event lineage:
   ```cypher
   MATCH (w:Workflow)<-[:BELONGS_TO]-(e:Event) RETURN w, e
   ```
3. Browse **MinIO** at http://localhost:9001 for the annotated image in the `images` bucket

## Running Tests

Each service has unit tests that run on the host (no Docker required).

**Node.js services** (workflow-api, image-fetcher, storage-service, notification-service):

```bash
cd services/<name>
npm install   # first time only
npm test
```

**Python services** (metadata-extractor, object-detection, image-annotator):

```bash
cd services/<name>
pip install -r requirements.txt   # first time only
python -m pytest tests/
```

## Event Envelope

All events follow this schema:

```json
{
  "eventId": "uuid",
  "eventType": "image.fetched",
  "workflowId": "uuid",
  "timestamp": "ISO-8601",
  "payload": {}
}
```

## Neo4j Data Model

```
(:Workflow {id, email}) <-[:BELONGS_TO]- (:Event {id, type, timestamp})
(:Event)-[:TRIGGERS]->(:Event)
```

## Project Structure

```
├── docker-compose.yml
├── services/
│   ├── workflow-api/        Node.js — Fastify REST API
│   ├── image-fetcher/       Node.js — image download & normalization
│   ├── metadata-extractor/  Python — EXIF extraction
│   ├── object-detection/    Python — YOLOv8 object detection
│   ├── image-annotator/     Python — fan-in annotation
│   ├── storage-service/     Node.js — MinIO upload
│   └── notification-service/Node.js — email via MailHog
└── data/images/             shared volume (gitignored)
```
