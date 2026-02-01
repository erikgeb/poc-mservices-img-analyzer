const { v4: uuidv4 } = require('uuid');

const EXCHANGE = 'imageanalyzer.events';

function publish(channel, eventType, workflowId, payload) {
  const event = {
    eventId: uuidv4(),
    eventType,
    workflowId,
    timestamp: new Date().toISOString(),
    payload,
  };
  channel.publish(EXCHANGE, eventType, Buffer.from(JSON.stringify(event)));
  return event;
}

async function recordEvent(driver, event, prevEventId) {
  const session = driver.session();
  try {
    await session.run(
      `MERGE (w:Workflow {id: $wid})
       CREATE (e:Event {id: $eid, type: $type, timestamp: $ts})
       CREATE (e)-[:BELONGS_TO]->(w)`,
      { wid: event.workflowId, eid: event.eventId, type: event.eventType, ts: event.timestamp }
    );
    if (prevEventId) {
      await session.run(
        `MATCH (prev:Event {id: $prev}), (cur:Event {id: $cur})
         CREATE (prev)-[:TRIGGERS]->(cur)`,
        { prev: prevEventId, cur: event.eventId }
      );
    }
  } finally {
    await session.close();
  }
}

const USER_AGENT = 'ImageAnalyzer/1.0';

async function validateImageUrl(axios, imageUrl) {
  let ct;
  try {
    const head = await axios.head(imageUrl, { timeout: 5000, maxRedirects: 5, headers: { 'User-Agent': USER_AGENT } });
    ct = head.headers['content-type'] || '';
  } catch {
    const partial = await axios.get(imageUrl, {
      timeout: 5000,
      maxRedirects: 5,
      headers: { Range: 'bytes=0-0', 'User-Agent': USER_AGENT },
      responseType: 'arraybuffer',
    });
    ct = partial.headers['content-type'] || '';
  }
  if (!ct.startsWith('image/')) {
    throw new Error(`Not an image content-type: ${ct}`);
  }
}

async function handleCreateWorkflow(req, reply, { axios, driver, channel }) {
  const { imageUrl, email } = req.body || {};
  if (!imageUrl || !email) {
    return reply.code(400).send({ error: 'imageUrl and email are required' });
  }

  try {
    await validateImageUrl(axios, imageUrl);
  } catch (err) {
    return reply.code(400).send({ error: err.message.startsWith('Not an image')
      ? err.message
      : `Cannot reach image URL: ${err.message}` });
  }

  const workflowId = uuidv4();

  const session = driver.session();
  try {
    await session.run(
      'MERGE (w:Workflow {id: $wid}) SET w.email = $email',
      { wid: workflowId, email }
    );
  } finally {
    await session.close();
  }

  const event = publish(channel, 'workflow.started', workflowId, { imageUrl, email });
  await recordEvent(driver, event, null);

  return { workflowId, status: 'started' };
}

async function handleGetWorkflow(req, reply, { driver }) {
  const session = driver.session();
  try {
    const result = await session.run(
      `MATCH (w:Workflow {id: $wid})<-[:BELONGS_TO]-(e:Event)
       RETURN e.id AS id, e.type AS type, e.timestamp AS timestamp
       ORDER BY e.timestamp`,
      { wid: req.params.id }
    );
    const events = result.records.map(r => ({
      id: r.get('id'),
      type: r.get('type'),
      timestamp: r.get('timestamp'),
    }));
    return { workflowId: req.params.id, events };
  } finally {
    await session.close();
  }
}

module.exports = { EXCHANGE, publish, recordEvent, validateImageUrl, handleCreateWorkflow, handleGetWorkflow };
