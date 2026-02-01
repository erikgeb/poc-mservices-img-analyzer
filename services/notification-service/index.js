const amqp = require('amqplib');
const neo4j = require('neo4j-driver');
const nodemailer = require('nodemailer');
const { v4: uuidv4 } = require('uuid');

const EXCHANGE = 'imageanalyzer.events';

const driver = neo4j.driver(
  process.env.NEO4J_URI,
  neo4j.auth.basic(process.env.NEO4J_USER, process.env.NEO4J_PASSWORD)
);

const transporter = nodemailer.createTransport({
  host: process.env.SMTP_HOST,
  port: parseInt(process.env.SMTP_PORT),
  secure: false,
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

async function getWorkflowEmail(workflowId) {
  const session = driver.session();
  try {
    const result = await session.run(
      'MATCH (w:Workflow {id: $wid}) RETURN w.email AS email',
      { wid: workflowId }
    );
    if (result.records.length > 0) {
      return result.records[0].get('email');
    }
    return null;
  } finally {
    await session.close();
  }
}

async function start() {
  const conn = await amqp.connect(process.env.RABBITMQ_URL);
  const channel = await conn.createChannel();
  await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
  const q = await channel.assertQueue('notification-service', { durable: true });
  await channel.bindQueue(q.queue, EXCHANGE, 'image.stored');

  console.log('Notification Service waiting for messages...');
  channel.consume(q.queue, async (msg) => {
    const event = JSON.parse(msg.content.toString());
    const { workflowId, payload } = event;
    const { presignedUrl } = payload;

    console.log(`Sending notification for workflow ${workflowId}`);
    try {
      const email = await getWorkflowEmail(workflowId);
      if (!email) {
        console.error(`No email found for workflow ${workflowId}`);
        channel.ack(msg);
        return;
      }

      await transporter.sendMail({
        from: '"Image Analyzer" <analyzer@example.com>',
        to: email,
        subject: `Image Analysis Complete - ${workflowId}`,
        html: `
          <h1>Image Analysis Complete</h1>
          <p><strong>Workflow ID:</strong> ${workflowId}</p>
          <p><strong>Completed at:</strong> ${new Date().toISOString()}</p>
          <p><a href="${presignedUrl}">Download Annotated Image</a></p>
        `,
      });

      const outEvent = {
        eventId: uuidv4(),
        eventType: 'notification.sent',
        workflowId,
        timestamp: new Date().toISOString(),
        payload: { recipient: email },
      };
      await recordEvent(outEvent, 'image.stored');
      console.log(`Notification sent to ${email}`);
      channel.ack(msg);
    } catch (err) {
      console.error(`Error sending notification for ${workflowId}:`, err.message);
      channel.ack(msg);
    }
  });
}

start().catch(err => { console.error(err); process.exit(1); });
