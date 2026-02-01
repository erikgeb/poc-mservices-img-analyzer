const amqp = require('amqplib');
const neo4j = require('neo4j-driver');
const Minio = require('minio');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const path = require('path');

const EXCHANGE = 'imageanalyzer.events';
const IMAGES_DIR = '/data/images';
const BUCKET = 'images';

const driver = neo4j.driver(
  process.env.NEO4J_URI,
  neo4j.auth.basic(process.env.NEO4J_USER, process.env.NEO4J_PASSWORD)
);

const minioClient = new Minio.Client({
  endPoint: process.env.MINIO_ENDPOINT,
  port: parseInt(process.env.MINIO_PORT),
  useSSL: false,
  accessKey: process.env.MINIO_ACCESS_KEY,
  secretKey: process.env.MINIO_SECRET_KEY,
});

// Separate client for presigned URLs only — signs with the public hostname so
// the browser's Host header matches the signature. Setting region avoids a
// getBucketRegion network call (which would fail against localhost from inside the container).
const publicUrl = new URL(process.env.MINIO_PUBLIC_URL || 'http://localhost:9000');
const minioPresignClient = new Minio.Client({
  endPoint: publicUrl.hostname,
  port: parseInt(publicUrl.port) || 9000,
  useSSL: publicUrl.protocol === 'https:',
  accessKey: process.env.MINIO_ACCESS_KEY,
  secretKey: process.env.MINIO_SECRET_KEY,
  region: 'us-east-1',
});

async function recordEvent(event, prevEventType) {
  const session = driver.session();
  try {
    await session.run(
      `MERGE (w:Workflow {id: $wid})
       CREATE (e:Event {id: $eid, type: $type, timestamp: $ts})
       CREATE (e)-[:BELONGS_TO]->(w)`,
      { wid: event.workflowId, eid: event.eventId, type: event.eventType, ts: event.timestamp }
    );
    if (prevEventType) {
      await session.run(
        `MATCH (w:Workflow {id: $wid})<-[:BELONGS_TO]-(prev:Event {type: $prevType})
         MATCH (cur:Event {id: $curId})
         CREATE (prev)-[:TRIGGERS]->(cur)`,
        { wid: event.workflowId, prevType: prevEventType, curId: event.eventId }
      );
    }
  } finally {
    await session.close();
  }
}

async function ensureBucket() {
  const exists = await minioClient.bucketExists(BUCKET);
  if (!exists) {
    await minioClient.makeBucket(BUCKET);
  }
}

async function start() {
  await ensureBucket();

  const conn = await amqp.connect(process.env.RABBITMQ_URL);
  const channel = await conn.createChannel();
  await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
  const q = await channel.assertQueue('storage-service', { durable: true });
  await channel.bindQueue(q.queue, EXCHANGE, 'image.annotated');

  console.log('Storage Service waiting for messages...');
  channel.consume(q.queue, async (msg) => {
    const event = JSON.parse(msg.content.toString());
    const { workflowId, payload } = event;
    const { filename } = payload;
    const filepath = path.join(IMAGES_DIR, filename);
    const objectKey = `annotated/${filename}`;

    console.log(`Uploading ${filename} to MinIO`);
    try {
      await minioClient.fPutObject(BUCKET, objectKey, filepath);
      const presignedUrl = await minioPresignClient.presignedGetObject(BUCKET, objectKey, 7 * 24 * 60 * 60);

      const outEvent = {
        eventId: uuidv4(),
        eventType: 'image.stored',
        workflowId,
        timestamp: new Date().toISOString(),
        payload: { bucket: BUCKET, objectKey, presignedUrl },
      };
      channel.publish(EXCHANGE, 'image.stored', Buffer.from(JSON.stringify(outEvent)));
      await recordEvent(outEvent, 'image.annotated');
      console.log(`Stored ${objectKey} in MinIO`);
      channel.ack(msg);
    } catch (err) {
      console.error(`Error storing ${filename}:`, err.message);
      channel.ack(msg);
    }
  });
}

start().catch(err => { console.error(err); process.exit(1); });
