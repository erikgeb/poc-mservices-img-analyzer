const path = require('path');
const { v4: uuidv4 } = require('uuid');

const EXCHANGE = 'imageanalyzer.events';

async function recordEvent(driver, event, prevEventType, workflowId) {
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

async function fetchAndProcessImage({ axios, sharp, fs, imageUrl, workflowId, imagesDir }) {
  const response = await axios.get(imageUrl, {
    responseType: 'arraybuffer',
    maxContentLength: 10 * 1024 * 1024,
    timeout: 30000,
  });

  const filename = `${workflowId}.jpg`;
  const outPath = path.join(imagesDir, filename);
  const image = sharp(Buffer.from(response.data));
  const meta = await image.metadata();

  let processed = image;
  if (meta.width > 1920 || meta.height > 1920) {
    processed = image.resize(1920, 1920, { fit: 'inside' });
  }
  await processed.jpeg().toFile(outPath);

  const finalMeta = await sharp(outPath).metadata();
  return {
    filename,
    width: finalMeta.width,
    height: finalMeta.height,
    mimeType: 'image/jpeg',
  };
}

function buildFetchedEvent(workflowId, payload) {
  return {
    eventId: uuidv4(),
    eventType: 'image.fetched',
    workflowId,
    timestamp: new Date().toISOString(),
    payload,
  };
}

async function handleMessage({ channel, driver, axios, sharp, fs, imagesDir }, msg) {
  const event = JSON.parse(msg.content.toString());
  const { workflowId, payload } = event;
  const { imageUrl } = payload;

  const result = await fetchAndProcessImage({ axios, sharp, fs, imageUrl, workflowId, imagesDir });
  const outEvent = buildFetchedEvent(workflowId, result);
  channel.publish(EXCHANGE, 'image.fetched', Buffer.from(JSON.stringify(outEvent)));
  await recordEvent(driver, outEvent, 'workflow.started', workflowId);
  return outEvent;
}

module.exports = { EXCHANGE, recordEvent, fetchAndProcessImage, buildFetchedEvent, handleMessage };
