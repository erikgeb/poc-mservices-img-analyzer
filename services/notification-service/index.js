const amqp = require('amqplib');
const neo4j = require('neo4j-driver');
const nodemailer = require('nodemailer');
const { EXCHANGE, handleMessage } = require('./lib');

const driver = neo4j.driver(
  process.env.NEO4J_URI,
  neo4j.auth.basic(process.env.NEO4J_USER, process.env.NEO4J_PASSWORD)
);

const transporter = nodemailer.createTransport({
  host: process.env.SMTP_HOST,
  port: parseInt(process.env.SMTP_PORT),
  secure: false,
});

async function start() {
  const conn = await amqp.connect(process.env.RABBITMQ_URL);
  const channel = await conn.createChannel();
  await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
  const q = await channel.assertQueue('notification-service', { durable: true });
  await channel.bindQueue(q.queue, EXCHANGE, 'image.stored');

  const deps = { driver, transporter, channel };

  console.log('Notification Service waiting for messages...');
  channel.consume(q.queue, async (msg) => {
    const event = JSON.parse(msg.content.toString());
    try {
      const result = await handleMessage(deps, msg);
      if (!result) {
        console.error(`No email found for workflow ${event.workflowId}`);
      } else {
        console.log(`Notification sent to ${result.payload.recipient}`);
      }
      channel.ack(msg);
    } catch (err) {
      console.error(`Error sending notification for ${event.workflowId}:`, err.message);
      channel.ack(msg);
    }
  });
}

start().catch(err => { console.error(err); process.exit(1); });
