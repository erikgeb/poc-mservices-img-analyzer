const { ensureBucket, handleMessage, recordEvent } = require('../lib');

jest.mock('uuid', () => ({ v4: () => 'test-uuid' }));

describe('ensureBucket', () => {
  it('creates bucket when it does not exist', async () => {
    const client = { bucketExists: jest.fn().mockResolvedValue(false), makeBucket: jest.fn().mockResolvedValue() };
    await ensureBucket(client);
    expect(client.makeBucket).toHaveBeenCalledWith('images');
  });

  it('skips creation when bucket exists', async () => {
    const client = { bucketExists: jest.fn().mockResolvedValue(true), makeBucket: jest.fn() };
    await ensureBucket(client);
    expect(client.makeBucket).not.toHaveBeenCalled();
  });
});

describe('handleMessage', () => {
  function setup() {
    const session = { run: jest.fn().mockResolvedValue(), close: jest.fn().mockResolvedValue() };
    const driver = { session: () => session };
    const channel = { publish: jest.fn() };
    const minioClient = { fPutObject: jest.fn().mockResolvedValue() };
    const minioPresignClient = { presignedGetObject: jest.fn().mockResolvedValue('http://presigned-url') };
    const msg = {
      content: Buffer.from(JSON.stringify({
        workflowId: 'wf-1',
        payload: { filename: 'wf-1_annotated.jpg' },
      })),
    };
    return { channel, driver, minioClient, minioPresignClient, msg, session };
  }

  it('uploads file and publishes image.stored event', async () => {
    const { channel, driver, minioClient, minioPresignClient, msg } = setup();
    const result = await handleMessage(
      { channel, driver, minioClient, minioPresignClient, imagesDir: '/data/images' },
      msg
    );
    expect(minioClient.fPutObject).toHaveBeenCalledWith('images', 'annotated/wf-1_annotated.jpg', expect.any(String));
    expect(result.eventType).toBe('image.stored');
    expect(result.payload.presignedUrl).toBe('http://presigned-url');
    expect(channel.publish).toHaveBeenCalled();
  });

  it('generates presigned URL with 7-day expiry', async () => {
    const { channel, driver, minioClient, minioPresignClient, msg } = setup();
    await handleMessage({ channel, driver, minioClient, minioPresignClient, imagesDir: '/data/images' }, msg);
    expect(minioPresignClient.presignedGetObject).toHaveBeenCalledWith(
      'images', 'annotated/wf-1_annotated.jpg', 7 * 24 * 60 * 60
    );
  });
});

describe('recordEvent', () => {
  it('records event in Neo4j', async () => {
    const session = { run: jest.fn().mockResolvedValue(), close: jest.fn().mockResolvedValue() };
    const driver = { session: () => session };
    const event = { workflowId: 'wf-1', eventId: 'e-1', eventType: 'image.stored', timestamp: 't' };
    await recordEvent(driver, event, 'image.annotated');
    expect(session.run).toHaveBeenCalledTimes(2);
  });
});
