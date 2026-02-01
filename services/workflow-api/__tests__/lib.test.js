const { publish, recordEvent, validateImageUrl, handleCreateWorkflow, handleGetWorkflow } = require('../lib');

jest.mock('uuid', () => ({ v4: () => 'test-uuid' }));

describe('publish', () => {
  it('publishes event envelope to exchange', () => {
    const channel = { publish: jest.fn() };
    const event = publish(channel, 'workflow.started', 'wf-1', { imageUrl: 'http://x.com/img.jpg' });

    expect(event.eventId).toBe('test-uuid');
    expect(event.eventType).toBe('workflow.started');
    expect(event.workflowId).toBe('wf-1');
    expect(event.payload).toEqual({ imageUrl: 'http://x.com/img.jpg' });
    expect(event.timestamp).toBeDefined();
    expect(channel.publish).toHaveBeenCalledWith(
      'imageanalyzer.events',
      'workflow.started',
      expect.any(Buffer)
    );
  });
});

describe('recordEvent', () => {
  function mockDriver() {
    const session = { run: jest.fn().mockResolvedValue(), close: jest.fn().mockResolvedValue() };
    return { session: () => session, _session: session };
  }

  it('creates event and workflow nodes', async () => {
    const drv = mockDriver();
    const event = { workflowId: 'wf-1', eventId: 'e-1', eventType: 'workflow.started', timestamp: 't' };
    await recordEvent(drv, event, null);

    expect(drv._session.run).toHaveBeenCalledTimes(1);
    expect(drv._session.close).toHaveBeenCalled();
  });

  it('creates TRIGGERS relationship when prevEventId is given', async () => {
    const drv = mockDriver();
    const event = { workflowId: 'wf-1', eventId: 'e-2', eventType: 'image.fetched', timestamp: 't' };
    await recordEvent(drv, event, 'e-1');

    expect(drv._session.run).toHaveBeenCalledTimes(2);
  });
});

describe('validateImageUrl', () => {
  it('succeeds when HEAD returns image content-type', async () => {
    const axios = { head: jest.fn().mockResolvedValue({ headers: { 'content-type': 'image/jpeg' } }) };
    await expect(validateImageUrl(axios, 'http://example.com/img.jpg')).resolves.toBeUndefined();
  });

  it('falls back to GET when HEAD fails', async () => {
    const axios = {
      head: jest.fn().mockRejectedValue(new Error('405')),
      get: jest.fn().mockResolvedValue({ headers: { 'content-type': 'image/png' } }),
    };
    await expect(validateImageUrl(axios, 'http://example.com/img.png')).resolves.toBeUndefined();
    expect(axios.get).toHaveBeenCalled();
  });

  it('rejects non-image content-type', async () => {
    const axios = { head: jest.fn().mockResolvedValue({ headers: { 'content-type': 'text/html' } }) };
    await expect(validateImageUrl(axios, 'http://example.com')).rejects.toThrow('Not an image content-type');
  });

  it('rejects unreachable URL', async () => {
    const axios = {
      head: jest.fn().mockRejectedValue(new Error('ECONNREFUSED')),
      get: jest.fn().mockRejectedValue(new Error('ECONNREFUSED')),
    };
    await expect(validateImageUrl(axios, 'http://down.com')).rejects.toThrow('ECONNREFUSED');
  });
});

describe('handleCreateWorkflow', () => {
  function setup(axiosOverrides = {}) {
    const session = { run: jest.fn().mockResolvedValue(), close: jest.fn().mockResolvedValue() };
    const driver = { session: () => session, _session: session };
    const channel = { publish: jest.fn() };
    const axios = { head: jest.fn().mockResolvedValue({ headers: { 'content-type': 'image/jpeg' } }), ...axiosOverrides };
    const reply = { code: jest.fn().mockReturnThis(), send: jest.fn() };
    return { driver, channel, axios, reply, session };
  }

  it('returns 400 when imageUrl is missing', async () => {
    const { reply, driver, channel, axios } = setup();
    await handleCreateWorkflow({ body: { email: 'a@b.com' } }, reply, { axios, driver, channel });
    expect(reply.code).toHaveBeenCalledWith(400);
  });

  it('returns 400 when email is missing', async () => {
    const { reply, driver, channel, axios } = setup();
    await handleCreateWorkflow({ body: { imageUrl: 'http://x.com/i.jpg' } }, reply, { axios, driver, channel });
    expect(reply.code).toHaveBeenCalledWith(400);
  });

  it('returns workflowId and status on success', async () => {
    const { reply, driver, channel, axios } = setup();
    const result = await handleCreateWorkflow(
      { body: { imageUrl: 'http://x.com/i.jpg', email: 'a@b.com' } },
      reply,
      { axios, driver, channel }
    );
    expect(result).toEqual({ workflowId: 'test-uuid', status: 'started' });
    expect(channel.publish).toHaveBeenCalled();
  });

  it('stores email on Workflow node', async () => {
    const { reply, driver, channel, axios, session } = setup();
    await handleCreateWorkflow(
      { body: { imageUrl: 'http://x.com/i.jpg', email: 'a@b.com' } },
      reply,
      { axios, driver, channel }
    );
    expect(session.run).toHaveBeenCalledWith(
      expect.stringContaining('SET w.email'),
      expect.objectContaining({ email: 'a@b.com' })
    );
  });
});

describe('handleGetWorkflow', () => {
  it('returns workflow events', async () => {
    const records = [
      { get: (k) => ({ id: 'e1', type: 'workflow.started', timestamp: 't1' }[k]) },
    ];
    const session = {
      run: jest.fn().mockResolvedValue({ records }),
      close: jest.fn().mockResolvedValue(),
    };
    const driver = { session: () => session };
    const result = await handleGetWorkflow({ params: { id: 'wf-1' } }, {}, { driver });
    expect(result.workflowId).toBe('wf-1');
    expect(result.events).toHaveLength(1);
    expect(result.events[0].type).toBe('workflow.started');
  });
});
