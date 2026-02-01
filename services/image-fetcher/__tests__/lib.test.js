const { fetchAndProcessImage, buildFetchedEvent, recordEvent, handleMessage } = require('../lib');

jest.mock('uuid', () => ({ v4: () => 'test-uuid' }));

describe('fetchAndProcessImage', () => {
  function mockSharp(metaWidth = 800, metaHeight = 600) {
    const instance = {
      metadata: jest.fn().mockResolvedValue({ width: metaWidth, height: metaHeight }),
      resize: jest.fn().mockReturnThis(),
      jpeg: jest.fn().mockReturnThis(),
      toFile: jest.fn().mockResolvedValue(),
    };
    const sharpFn = jest.fn().mockReturnValue(instance);
    sharpFn._instance = instance;
    return sharpFn;
  }

  it('downloads image and converts to JPEG', async () => {
    const sharp = mockSharp();
    const axios = { get: jest.fn().mockResolvedValue({ data: Buffer.from('img') }) };
    const result = await fetchAndProcessImage({
      axios, sharp, fs: {}, imageUrl: 'http://x.com/i.jpg', workflowId: 'wf-1', imagesDir: '/tmp',
    });
    expect(result.filename).toBe('wf-1.jpg');
    expect(result.mimeType).toBe('image/jpeg');
    expect(sharp._instance.jpeg).toHaveBeenCalled();
  });

  it('resizes images larger than 1920px', async () => {
    const sharp = mockSharp(3000, 2000);
    const axios = { get: jest.fn().mockResolvedValue({ data: Buffer.from('img') }) };
    await fetchAndProcessImage({
      axios, sharp, fs: {}, imageUrl: 'http://x.com/i.jpg', workflowId: 'wf-1', imagesDir: '/tmp',
    });
    expect(sharp._instance.resize).toHaveBeenCalledWith(1920, 1920, { fit: 'inside' });
  });

  it('does not resize images within 1920px', async () => {
    const sharp = mockSharp(1920, 1080);
    const axios = { get: jest.fn().mockResolvedValue({ data: Buffer.from('img') }) };
    await fetchAndProcessImage({
      axios, sharp, fs: {}, imageUrl: 'http://x.com/i.jpg', workflowId: 'wf-1', imagesDir: '/tmp',
    });
    expect(sharp._instance.resize).not.toHaveBeenCalled();
  });

  it('throws on download failure', async () => {
    const sharp = mockSharp();
    const axios = { get: jest.fn().mockRejectedValue(new Error('Network error')) };
    await expect(fetchAndProcessImage({
      axios, sharp, fs: {}, imageUrl: 'http://x.com/i.jpg', workflowId: 'wf-1', imagesDir: '/tmp',
    })).rejects.toThrow('Network error');
  });
});

describe('buildFetchedEvent', () => {
  it('builds correct event envelope', () => {
    const event = buildFetchedEvent('wf-1', { filename: 'wf-1.jpg', width: 800, height: 600, mimeType: 'image/jpeg' });
    expect(event.eventId).toBe('test-uuid');
    expect(event.eventType).toBe('image.fetched');
    expect(event.workflowId).toBe('wf-1');
    expect(event.payload.filename).toBe('wf-1.jpg');
  });
});

describe('recordEvent', () => {
  it('records event in Neo4j with TRIGGERS when prevEventType given', async () => {
    const session = { run: jest.fn().mockResolvedValue(), close: jest.fn().mockResolvedValue() };
    const driver = { session: () => session };
    const event = { workflowId: 'wf-1', eventId: 'e-1', eventType: 'image.fetched', timestamp: 't' };
    await recordEvent(driver, event, 'workflow.started', 'wf-1');
    expect(session.run).toHaveBeenCalledTimes(2);
    expect(session.close).toHaveBeenCalled();
  });
});

describe('handleMessage', () => {
  it('processes message end-to-end', async () => {
    const session = { run: jest.fn().mockResolvedValue(), close: jest.fn().mockResolvedValue() };
    const driver = { session: () => session };
    const channel = { publish: jest.fn() };
    const sharpInstance = {
      metadata: jest.fn().mockResolvedValue({ width: 800, height: 600 }),
      resize: jest.fn().mockReturnThis(),
      jpeg: jest.fn().mockReturnThis(),
      toFile: jest.fn().mockResolvedValue(),
    };
    const sharp = jest.fn().mockReturnValue(sharpInstance);
    const axios = { get: jest.fn().mockResolvedValue({ data: Buffer.from('img') }) };
    const msg = {
      content: Buffer.from(JSON.stringify({
        workflowId: 'wf-1',
        payload: { imageUrl: 'http://x.com/i.jpg' },
      })),
    };

    const result = await handleMessage({ channel, driver, axios, sharp, fs: {}, imagesDir: '/tmp' }, msg);
    expect(result.eventType).toBe('image.fetched');
    expect(channel.publish).toHaveBeenCalled();
  });
});
