const amqp = require('amqplib');
const neo4j = require('neo4j-driver');
const sharp = require('sharp');
const axios = require('axios');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const path = require('path');

const EXCHANGE = 'imageanalyzer.events';
const IMAGES_DIR = '/data/images';
const driver = neo4j.driver(
  process.env.NEO4J_URI,
  neo4j.auth.basic(process.env.NEO4J_USER, process.env.NEO4J_PASSWORD)
);

async function recordEvent(event, prevEventType, workflowId) {
  const session = driver.session();
  try {
    await session.run(
      `MERGE (w:Workflow {id: $wid})
       CREATE (e:Event {id: $eid, type: $type, timestamp: $ts})
       CREATE (e)-[:BELONGS_TO]->(w)`,
      { wid: workflowId, eid: event.eventId, type: event.eventType, ts: event.timestamp }
    );
    if (prevEventType) {
      await session.run(
        `MATCH (w:Workflow {id: $wid})<-[:BELONGS_TO]-(prev:Event {type: $prevType})
         MATCH (cur:Event {id: $curId})
         CREATE (prev)-[:TRIGGERS]->(cur)`,
        { wid: workflowId, prevType: prevEventType, curId: event.eventId }
      );
    }
  } finally {
    await session.close();
  }
}

async function start() {
  const conn = await amqp.connect(process.env.RABBITMQ_URL);
  const channel = await conn.createChannel();
  await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
  const q = await channel.assertQueue('image-fetcher', { durable: true });
  await channel.bindQueue(q.queue, EXCHANGE, 'workflow.started');

  console.log('Image Fetcher waiting for messages...');
  channel.consume(q.queue, async (msg) => {
    const event = JSON.parse(msg.content.toString());
    const { workflowId, payload } = event;
    const { imageUrl } = payload;
    console.log(`Fetching image for workflow ${workflowId}`);

    try {
      const response = await axios.get(imageUrl, {
        responseType: 'arraybuffer',
        maxContentLength: 10 * 1024 * 1024,
        timeout: 30000,
      });

      const filename = `${workflowId}.jpg`;
      const outPath = path.join(IMAGES_DIR, filename);
      const image = sharp(Buffer.from(response.data));
      const meta = await image.metadata();

      let processed = image;
      if (meta.width > 1920 || meta.height > 1920) {
        processed = image.resize(1920, 1920, { fit: 'inside' });
      }
      await processed.jpeg().toFile(outPath);

      const finalMeta = await sharp(outPath).metadata();
      const outEvent = {
        eventId: uuidv4(),
        eventType: 'image.fetched',
        workflowId,
        timestamp: new Date().toISOString(),
        payload: {
          filename,
          width: finalMeta.width,
          height: finalMeta.height,
          mimeType: 'image/jpeg',
        },
      };
      channel.publish(EXCHANGE, 'image.fetched', Buffer.from(JSON.stringify(outEvent)));
      await recordEvent(outEvent, 'workflow.started', workflowId);
      console.log(`Image fetched and saved: ${filename}`);
      channel.ack(msg);
    } catch (err) {
      console.error(`Error fetching image for ${workflowId}:`, err.message);
      channel.ack(msg);
    }
  });
}

start().catch(err => { console.error(err); process.exit(1); });
