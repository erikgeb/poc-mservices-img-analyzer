const { v4: uuidv4 } = require('uuid');

const EXCHANGE = 'imageanalyzer.events';

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

async function getWorkflowEmail(driver, workflowId) {
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

async function handleMessage({ driver, transporter, channel }, msg) {
  const event = JSON.parse(msg.content.toString());
  const { workflowId, payload } = event;
  const { presignedUrl } = payload;

  const email = await getWorkflowEmail(driver, workflowId);
  if (!email) {
    return null;
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
  await recordEvent(driver, outEvent, 'image.stored');
  return outEvent;
}

module.exports = { EXCHANGE, recordEvent, getWorkflowEmail, handleMessage };
