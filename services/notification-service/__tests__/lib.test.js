const { getWorkflowEmail, handleMessage, recordEvent } = require('../lib');

jest.mock('uuid', () => ({ v4: () => 'test-uuid' }));

describe('getWorkflowEmail', () => {
  it('returns email from Neo4j', async () => {
    const session = {
      run: jest.fn().mockResolvedValue({ records: [{ get: () => 'a@b.com' }] }),
      close: jest.fn().mockResolvedValue(),
    };
    const driver = { session: () => session };
    const email = await getWorkflowEmail(driver, 'wf-1');
    expect(email).toBe('a@b.com');
  });

  it('returns null when no workflow found', async () => {
    const session = {
      run: jest.fn().mockResolvedValue({ records: [] }),
      close: jest.fn().mockResolvedValue(),
    };
    const driver = { session: () => session };
    const email = await getWorkflowEmail(driver, 'wf-missing');
    expect(email).toBeNull();
  });
});

describe('handleMessage', () => {
  function setup(email = 'a@b.com') {
    const session = {
      run: jest.fn().mockImplementation((query) => {
        if (query.includes('RETURN w.email')) {
          return { records: email ? [{ get: () => email }] : [] };
        }
        return {};
      }),
      close: jest.fn().mockResolvedValue(),
    };
    const driver = { session: () => session };
    const transporter = { sendMail: jest.fn().mockResolvedValue() };
    const channel = {};
    const msg = {
      content: Buffer.from(JSON.stringify({
        workflowId: 'wf-1',
        payload: { presignedUrl: 'http://presigned' },
      })),
    };
    return { driver, transporter, channel, msg, session };
  }

  it('sends email with correct recipient and content', async () => {
    const { driver, transporter, channel, msg } = setup();
    const result = await handleMessage({ driver, transporter, channel }, msg);
    expect(transporter.sendMail).toHaveBeenCalledWith(expect.objectContaining({
      to: 'a@b.com',
      subject: expect.stringContaining('wf-1'),
      html: expect.stringContaining('presigned'),
    }));
    expect(result.payload.recipient).toBe('a@b.com');
  });

  it('returns null when no email found', async () => {
    const { driver, transporter, channel, msg } = setup(null);
    const result = await handleMessage({ driver, transporter, channel }, msg);
    expect(result).toBeNull();
    expect(transporter.sendMail).not.toHaveBeenCalled();
  });
});

describe('recordEvent', () => {
  it('records event with TRIGGERS', async () => {
    const session = { run: jest.fn().mockResolvedValue(), close: jest.fn().mockResolvedValue() };
    const driver = { session: () => session };
    const event = { workflowId: 'wf-1', eventId: 'e-1', eventType: 'notification.sent', timestamp: 't' };
    await recordEvent(driver, event, 'image.stored');
    expect(session.run).toHaveBeenCalledTimes(2);
  });
});
