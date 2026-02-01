const amqp = require('amqplib');
const neo4j = require('neo4j-driver');
const Minio = require('minio');
const { EXCHANGE, ensureBucket, handleMessage } = require('./lib');

const IMAGES_DIR = '/data/images';

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

const publicUrl = new URL(process.env.MINIO_PUBLIC_URL || 'http://localhost:9000');
const minioPresignClient = new Minio.Client({
  endPoint: publicUrl.hostname,
  port: parseInt(publicUrl.port) || 9000,
  useSSL: publicUrl.protocol === 'https:',
  accessKey: process.env.MINIO_ACCESS_KEY,
  secretKey: process.env.MINIO_SECRET_KEY,
  region: 'us-east-1',
});

async function start() {
  await ensureBucket(minioClient);

  const conn = await amqp.connect(process.env.RABBITMQ_URL);
  const channel = await conn.createChannel();
  await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
  const q = await channel.assertQueue('storage-service', { durable: true });
  await channel.bindQueue(q.queue, EXCHANGE, 'image.annotated');

  const deps = { channel, driver, minioClient, minioPresignClient, imagesDir: IMAGES_DIR };

  console.log('Storage Service waiting for messages...');
  channel.consume(q.queue, async (msg) => {
    try {
      const outEvent = await handleMessage(deps, msg);
      console.log(`Stored ${outEvent.payload.objectKey} in MinIO`);
      channel.ack(msg);
    } catch (err) {
      console.error(`Error storing:`, err.message);
      channel.ack(msg);
    }
  });
}

start().catch(err => { console.error(err); process.exit(1); });
