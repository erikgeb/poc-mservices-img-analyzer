const path = require('path');
const { v4: uuidv4 } = require('uuid');

const EXCHANGE = 'imageanalyzer.events';
const BUCKET = 'images';

async function recordEvent(driver, event, prevEventType) {
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

async function ensureBucket(minioClient) {
  const exists = await minioClient.bucketExists(BUCKET);
  if (!exists) {
    await minioClient.makeBucket(BUCKET);
  }
}

async function handleMessage({ channel, driver, minioClient, minioPresignClient, imagesDir }, msg) {
  const event = JSON.parse(msg.content.toString());
  const { workflowId, payload } = event;
  const { filename } = payload;
  const filepath = path.join(imagesDir, filename);
  const objectKey = `annotated/${filename}`;

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
  await recordEvent(driver, outEvent, 'image.annotated');
  return outEvent;
}

module.exports = { EXCHANGE, BUCKET, recordEvent, ensureBucket, handleMessage };
