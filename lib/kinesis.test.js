const { PutRecordsCommand, KinesisClient } = require("@aws-sdk/client-kinesis");
require("aws-sdk-client-mock-jest");
const { mockClient } = require("aws-sdk-client-mock");
const log = require('lambda-log')
const kinesis = require('./kinesis')
const lock = require('./kinesis-lock')
const RedisBackup = require('./redis-backup')

describe('kinesis', () => {
  let redis, overall, segment
  beforeEach(() => {
    redis = new RedisBackup(process.env.REDIS_URL)
    overall = { le: '1234', digest: '5678', time: 9, bytes: 10, seconds: 12, percent: 0.4 }
    segment = { ...overall, segment: 2 }
    process.env.KINESIS_IMPRESSION_STREAM = 'some-good-stream'
  })

  afterEach(async () => {
    await redis.nuke('dtcounts:imp:1234:*')
    await redis.nuke('dtcounts:imp:1235:*')
    await redis.disconnect()
    jest.restoreAllMocks()
  })

  // mock each kinesis.putRecords() batch call, and success/failure statuses
  function mockPutRecords(...isSuccessList) {
    const result = {
      Records: isSuccessList.map((isSuccess) => {
        if (isSuccess === true) {
          return { SequenceNumber: 'anything', ShardId: 'anything' }
        } else {
          return { ErrorCode: 'anything', ErrorMessage: 'anything' }
        }
      })
    }
    return result
  }

  // manually check for a lock
  function isLocked(rec) {
    return lock.isLocked(redis, kinesis.format(rec))
  }

  it("returns a kinesis client", async () => {
    const client = await kinesis.client();
    expect(client).toBeInstanceOf(KinesisClient);
  });

  it('formats overall download records', () => {
    expect(kinesis.format(overall)).toEqual({
      type: 'bytes',
      listenerEpisode: '1234',
      digest: '5678',
      timestamp: 9,
      bytes: 10,
      seconds: 12,
      percent: 0.4,
    })
  })

  it('formats segment impression records', () => {
    expect(kinesis.format(segment)).toEqual({
      type: 'segmentbytes',
      segment: 2,
      listenerEpisode: '1234',
      digest: '5678',
      timestamp: 9,
    })
  })

  it('rounds seconds and percents', () => {
    expect(kinesis.format({ ...overall, seconds: 0.123456, percent: 0.123456 })).toMatchObject({
      seconds: 0.12,
      percent: 0.1235,
    })
  })

  it('requires a kinesis stream env', async () => {
    try {
      process.env.KINESIS_IMPRESSION_STREAM = ''
      await kinesis.putWithLock(redis, [overall])
      fail('should have gotten an error')
    } catch (err) {
      expect(err.name).toEqual('MissingEnvError')
      expect(err.retryable).toEqual(true)
    }
  })

  it('puts nothing', async () => {
    expect(await kinesis.putWithLock(redis, [])).toMatchObject({ overall: 0, segments: 0 })
  })

  it('puts formatted records to kinesis', async () => {
    const kinesisMock = mockClient(KinesisClient)
    kinesisMock.on(PutRecordsCommand).resolves(mockPutRecords(true, true))

    expect(await kinesis.putWithLock(redis, [overall, segment])).toMatchObject({
      overall: 1,
      segments: 1,
    })

    expect(kinesisMock).toHaveReceivedCommandWith(PutRecordsCommand, {
      StreamName: 'some-good-stream',
      Records: [
        {
          PartitionKey: '1234',
          Data: JSON.stringify(kinesis.format(overall)),
        },
        {
          PartitionKey: '1234',
          Data: JSON.stringify(kinesis.format(segment)),
        },
      ],
    });
  })

  it('only puts an overall download once', async () => {
    const kinesisMock = mockClient(KinesisClient)
    kinesisMock.on(PutRecordsCommand).resolves(mockPutRecords(true))

    expect(await isLocked(overall)).toEqual(false)
    expect(await kinesis.putWithLock(redis, [overall, overall, overall])).toMatchObject({
      overall: 1,
    })
    expect(await kinesis.putWithLock(redis, [overall])).toMatchObject({ overall: 0 })
    expect(await kinesis.putWithLock(redis, [overall, overall])).toMatchObject({ overall: 0 })

    expect(kinesisMock).toHaveReceivedCommandWith(PutRecordsCommand, {
      StreamName: 'some-good-stream',
      Records: [
        {
          PartitionKey: '1234',
          Data: JSON.stringify(kinesis.format(overall)),
        }
      ],
    });

    expect(await isLocked(overall)).toEqual(true)
  })

  it('only puts a segment impression once', async () => {
    const kinesisMock = mockClient(KinesisClient)
    kinesisMock.on(PutRecordsCommand).resolves(mockPutRecords(true))

    expect(await isLocked(segment)).toEqual(false)
    expect(await kinesis.putWithLock(redis, [segment, segment])).toMatchObject({ segments: 1 })
    expect(await kinesis.putWithLock(redis, [segment, segment, segment])).toMatchObject({
      segments: 0,
    })
    expect(await kinesis.putWithLock(redis, [segment])).toMatchObject({ segments: 0 })

    expect(kinesisMock).toHaveReceivedCommandWith(PutRecordsCommand, {
      StreamName: 'some-good-stream',
      Records: [
        {
          PartitionKey: '1234',
          Data: JSON.stringify(kinesis.format(segment)),
        }
      ],
    });

    expect(await isLocked(segment)).toEqual(true)
  })

  it('unlocks kinesis partial failures', async () => {
    jest.spyOn(log, 'warn').mockImplementation(() => null)
    const kinesisMock = mockClient(KinesisClient)
    kinesisMock.on(PutRecordsCommand).resolves(mockPutRecords(false, true))

    expect(await kinesis.putWithLock(redis, [overall, segment])).toMatchObject({
      segments: 1,
      failed: 1,
    })

    expect(await isLocked(overall)).toEqual(false)
    expect(await isLocked(segment)).toEqual(true)

    expect(log.warn).toHaveBeenCalledTimes(1)
    expect(log.warn.mock.calls[0][0].message).toMatch('putRecords partial failure')
    expect(log.warn.mock.calls[0][1]).toMatchObject({ ErrorCode: 'anything' })

    expect(kinesisMock).toHaveReceivedCommandWith(PutRecordsCommand, {
      StreamName: 'some-good-stream',
      Records: [
        {
          PartitionKey: '1234',
          Data: JSON.stringify(kinesis.format(overall)),
        },
        {
          PartitionKey: '1234',
          Data: JSON.stringify(kinesis.format(segment)),
        }
      ],
    });

    kinesisMock.on(PutRecordsCommand).resolves(mockPutRecords(true))
    expect(await kinesis.putWithLock(redis, [overall, segment])).toMatchObject({ overall: 1 })

    expect(kinesisMock).toHaveReceivedCommandWith(PutRecordsCommand, {
      StreamName: 'some-good-stream',
      Records: [
        {
          PartitionKey: '1234',
          Data: JSON.stringify(kinesis.format(overall)),
        },
        {
          PartitionKey: '1234',
          Data: JSON.stringify(kinesis.format(segment)),
        }
      ],
    });

    expect(kinesisMock).toHaveReceivedCommandWith(PutRecordsCommand, {
      StreamName: 'some-good-stream',
      Records: [
        {
          PartitionKey: '1234',
          Data: JSON.stringify(kinesis.format(overall)),
        }
      ],
    });

    expect(await isLocked(overall)).toEqual(true)
    expect(await isLocked(segment)).toEqual(true)
  })

  it('unlocks kinesis complete failures', async () => {
    jest.spyOn(kinesis, '_putRecords').mockImplementation(async () => {
      throw new Error('Complete failure!')
    })

    jest.spyOn(log, 'warn').mockImplementation(() => null)
    expect(await kinesis.putWithLock(redis, [overall])).toMatchObject({ failed: 1 })
    expect(await isLocked(overall)).toEqual(false)

    expect(log.warn).toHaveBeenCalledTimes(1)
    expect(log.warn.mock.calls[0][0].message).toMatch('putRecords failure')
    expect(log.warn.mock.calls[0][1].Records.length).toEqual(1)
  })

  it('locks to the first digest downloaded', async () => {
    const kinesisMock = mockClient(KinesisClient)
    kinesisMock.on(PutRecordsCommand).resolves(mockPutRecords(true))

    expect(await kinesis.putWithLock(redis, [overall])).toMatchObject({ overall: 1 })

    expect(kinesisMock).toHaveReceivedCommandWith(PutRecordsCommand, {
      StreamName: 'some-good-stream',
      Records: [
        {
          PartitionKey: '1234',
          Data: JSON.stringify(kinesis.format(overall)),
        }
      ],
    });

    expect(await kinesis.putWithLock(redis, [{ ...overall, digest: '5679' }])).toMatchObject({
      overallDups: 1,
    })

    expect(kinesisMock).toHaveReceivedCommandWith(PutRecordsCommand, {
      StreamName: 'some-good-stream',
      Records: [
        {
          PartitionKey: '1234',
          Data: expect.stringContaining('digestCache'),
        }
      ],
    });
  })

  it('unfortunately does not unlock the digest for a failed putRecord', async () => {
    jest.spyOn(log, 'warn').mockImplementation(() => null)
    const kinesisMock = mockClient(KinesisClient)

    kinesisMock.on(PutRecordsCommand).resolves(mockPutRecords(false))
    expect(await kinesis.putWithLock(redis, [segment])).toMatchObject({ failed: 1 })

    expect(kinesisMock).toHaveReceivedCommandWith(PutRecordsCommand, {
      StreamName: 'some-good-stream',
      Records: [
        {
          PartitionKey: '1234',
          Data: JSON.stringify(kinesis.format(segment)),
        }
      ],
    });

    kinesisMock.on(PutRecordsCommand).resolves(mockPutRecords(true))
    expect(await kinesis.putWithLock(redis, [{ ...segment, digest: '5679' }])).toMatchObject({
      segmentDups: 1,
    })

    expect(kinesisMock).toHaveReceivedCommandWith(PutRecordsCommand, {
      StreamName: 'some-good-stream',
      Records: [
        {
          PartitionKey: '1234',
          Data: expect.stringContaining('digestCache'),
        }
      ],
    });
  })

  it('splits into batches', async () => {
    const kinesisMock = mockClient(KinesisClient)
    kinesisMock.on(PutRecordsCommand)
      .resolvesOnce(mockPutRecords(true))
      .resolvesOnce(mockPutRecords(true, true))
      .resolvesOnce(mockPutRecords(true))

    const manyRecords = [
      overall,
      overall,
      overall,
      segment,
      overall,
      { ...segment, le: '1235' },
      { ...overall, le: '1235' },
    ]

    expect(await kinesis.putWithLock(redis, manyRecords, 3)).toMatchObject({
      overall: 2,
      overallDups: 0,
      segments: 2,
      segmentDups: 0,
      failed: 0,
    })

    expect(kinesisMock).toHaveReceivedCommandTimes(PutRecordsCommand, 3);
  })
})
