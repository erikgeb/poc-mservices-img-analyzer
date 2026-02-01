const fastify = require('fastify')({ logger: true });
const amqp = require('amqplib');
const neo4j = require('neo4j-driver');
const axios = require('axios');
const { v4: uuidv4 } = require('uuid');

const EXCHANGE = 'imageanalyzer.events';
let channel;
const driver = neo4j.driver(
  process.env.NEO4J_URI,
  neo4j.auth.basic(process.env.NEO4J_USER, process.env.NEO4J_PASSWORD)
);

async function setupRabbit() {
  const conn = await amqp.connect(process.env.RABBITMQ_URL);
  channel = await conn.createChannel();
  await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
}

function publish(eventType, workflowId, payload) {
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

async function recordEvent(event, prevEventId) {
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

fastify.post('/workflows', async (req, reply) => {
  const { imageUrl, email } = req.body || {};
  if (!imageUrl || !email) {
    return reply.code(400).send({ error: 'imageUrl and email are required' });
  }

  // HEAD check
  try {
    const head = await axios.head(imageUrl, { timeout: 5000 });
    const ct = head.headers['content-type'] || '';
    if (!ct.startsWith('image/')) {
      return reply.code(400).send({ error: `Not an image content-type: ${ct}` });
    }
  } catch (err) {
    return reply.code(400).send({ error: `Cannot reach image URL: ${err.message}` });
  }

  const workflowId = uuidv4();

  // Store email on Workflow node
  const session = driver.session();
  try {
    await session.run(
      'MERGE (w:Workflow {id: $wid}) SET w.email = $email',
      { wid: workflowId, email }
    );
  } finally {
    await session.close();
  }

  const event = publish('workflow.started', workflowId, { imageUrl, email });
  await recordEvent(event, null);

  return { workflowId, status: 'started' };
});

fastify.get('/workflows/:id', async (req, reply) => {
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
});

fastify.get('/health', async () => ({ status: 'ok' }));

async function start() {
  await setupRabbit();
  await fastify.listen({ port: 3000, host: '0.0.0.0' });
}

start().catch(err => { console.error(err); process.exit(1); });
