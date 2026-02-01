const amqp = require('amqplib');
const neo4j = require('neo4j-driver');
const sharp = require('sharp');
const axios = require('axios');
const fs = require('fs');
const { EXCHANGE, handleMessage } = require('./lib');

const IMAGES_DIR = '/data/images';
const driver = neo4j.driver(
  process.env.NEO4J_URI,
  neo4j.auth.basic(process.env.NEO4J_USER, process.env.NEO4J_PASSWORD)
);

async function start() {
  const conn = await amqp.connect(process.env.RABBITMQ_URL);
  const channel = await conn.createChannel();
  await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
  const q = await channel.assertQueue('image-fetcher', { durable: true });
  await channel.bindQueue(q.queue, EXCHANGE, 'workflow.started');

  const deps = { channel, driver, axios, sharp, fs, imagesDir: IMAGES_DIR };

  console.log('Image Fetcher waiting for messages...');
  channel.consume(q.queue, async (msg) => {
    try {
      const outEvent = await handleMessage(deps, msg);
      console.log(`Image fetched and saved: ${outEvent.payload.filename}`);
      channel.ack(msg);
    } catch (err) {
      console.error(`Error fetching image:`, err.message);
      channel.ack(msg);
    }
  });
}

start().catch(err => { console.error(err); process.exit(1); });
