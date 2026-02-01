const fastify = require('fastify')({ logger: true });
const amqp = require('amqplib');
const neo4j = require('neo4j-driver');
const axios = require('axios');
const { handleCreateWorkflow, handleGetWorkflow } = require('./lib');

let channel;
const driver = neo4j.driver(
  process.env.NEO4J_URI,
  neo4j.auth.basic(process.env.NEO4J_USER, process.env.NEO4J_PASSWORD)
);

async function setupRabbit() {
  const conn = await amqp.connect(process.env.RABBITMQ_URL);
  channel = await conn.createChannel();
  await channel.assertExchange('imageanalyzer.events', 'topic', { durable: true });
}

fastify.post('/workflows', (req, reply) => handleCreateWorkflow(req, reply, { axios, driver, channel }));
fastify.get('/workflows/:id', (req, reply) => handleGetWorkflow(req, reply, { driver }));
fastify.get('/health', async () => ({ status: 'ok' }));

async function start() {
  await setupRabbit();
  await fastify.listen({ port: 3000, host: '0.0.0.0' });
}

start().catch(err => { console.error(err); process.exit(1); });
