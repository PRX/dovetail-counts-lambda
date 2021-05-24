const log = require('lambda-log')
const kinesis = require('./kinesis')
const lock = require('./kinesis-lock')
const RedisBackup = require('./redis-backup')

describe('kinesis', () => {

  let redis, overall, segment
  beforeEach(() => {
    redis = new RedisBackup(process.env.REDIS_URL)
    overall = {le: '1234', digest: '5678', time: 9, bytes: 10, seconds: 12, percent: 0.4}
    segment = {...overall, segment: 2}
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
    let idx = 0, calls = []
    jest.spyOn(kinesis, '_putRecords').mockImplementation(async function(args) {
      calls.push(args)
      return {Records: args.Records.map(() => {
        const isSuccess = isSuccessList[idx++]
        if (isSuccess === true) {
          return {SequenceNumber: 'anything', ShardId: 'anything'}
        } else if (isSuccess === false) {
          return {ErrorCode: 'anything', ErrorMessage: 'anything'}
        } else {
          throw new Error(`Unmocked kinesis result ${idx}`)
        }
      })}
    })
    return calls
  }

  // manually check for a lock
  function isLocked(rec) {
    return lock.isLocked(redis, kinesis.format(rec))
  }

  it('formats overall download records', () => {
    expect(kinesis.format(overall)).toEqual({
      type: 'bytes',
      listenerEpisode: '1234',
      digest: '5678',
      timestamp: 9,
      bytes: 10,
      seconds: 12,
      percent: 0.4
    })
  })

  it('formats segment impression records', () => {
    expect(kinesis.format(segment)).toEqual({
      type: 'segmentbytes',
      segment: 2,
      listenerEpisode: '1234',
      digest: '5678',
      timestamp: 9
    })
  })

  it('rounds seconds and percents', () => {
    expect(kinesis.format({...overall, seconds: 0.123456, percent: 0.123456})).toMatchObject({
      seconds: 0.12,
      percent: 0.1235
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
    expect(await kinesis.putWithLock(redis, [])).toMatchObject({overall: 0, segments: 0})
  })

  it('puts formatted records to kinesis', async () => {
    const calls = mockPutRecords(true, true)
    expect(await kinesis.putWithLock(redis, [overall, segment])).toMatchObject({overall: 1, segments: 1})
    expect(calls.length).toEqual(1)
    expect(calls[0].StreamName).toEqual('some-good-stream')
    expect(calls[0].Records.length).toEqual(2)
    expect(calls[0].Records[0].PartitionKey).toEqual('1234')
    expect(JSON.parse(calls[0].Records[0].Data)).toEqual(kinesis.format(overall))
    expect(calls[0].Records[1].PartitionKey).toEqual('1234')
    expect(JSON.parse(calls[0].Records[1].Data)).toEqual(kinesis.format(segment))
  })

  it('only puts an overall download once', async () => {
    const calls = mockPutRecords(true)
    expect(await isLocked(overall)).toEqual(false)
    expect(await kinesis.putWithLock(redis, [overall, overall, overall])).toMatchObject({overall: 1})
    expect(await kinesis.putWithLock(redis, [overall])).toMatchObject({overall: 0})
    expect(await kinesis.putWithLock(redis, [overall, overall])).toMatchObject({overall: 0})
    expect(calls.length).toEqual(1)
    expect(calls[0].Records.length).toEqual(1)
    expect(await isLocked(overall)).toEqual(true)
  })

  it('only puts a segment impression once', async () => {
    const calls = mockPutRecords(true)
    expect(await isLocked(segment)).toEqual(false)
    expect(await kinesis.putWithLock(redis, [segment, segment])).toMatchObject({segments: 1})
    expect(await kinesis.putWithLock(redis, [segment, segment, segment])).toMatchObject({segments: 0})
    expect(await kinesis.putWithLock(redis, [segment])).toMatchObject({segments: 0})
    expect(calls.length).toEqual(1)
    expect(calls[0].Records.length).toEqual(1)
    expect(await isLocked(segment)).toEqual(true)
  })

  it('unlocks kinesis partial failures', async () => {
    const calls = mockPutRecords(false, true, true)

    jest.spyOn(log, 'warn').mockImplementation(() => null)
    expect(await kinesis.putWithLock(redis, [overall, segment])).toMatchObject({segments: 1, failed: 1})
    expect(await isLocked(overall)).toEqual(false)
    expect(await isLocked(segment)).toEqual(true)

    expect(log.warn).toHaveBeenCalledTimes(1)
    expect(log.warn.mock.calls[0][0].message).toMatch('putRecords partial failure')
    expect(log.warn.mock.calls[0][1]).toMatchObject({ErrorCode: 'anything'})

    expect(await kinesis.putWithLock(redis, [overall, segment])).toMatchObject({overall: 1})
    expect(calls.length).toEqual(2)
    expect(calls[0].Records.length).toEqual(2) // overall=failed, segment=success
    expect(calls[1].Records.length).toEqual(1) // overall=success
    expect(await isLocked(overall)).toEqual(true)
    expect(await isLocked(segment)).toEqual(true)
  })

  it('unlocks kinesis complete failures', async () => {
    jest.spyOn(kinesis, '_putRecords').mockImplementation(async () => {
      throw new Error('Complete failure!')
    })

    jest.spyOn(log, 'warn').mockImplementation(() => null)
    expect(await kinesis.putWithLock(redis, [overall])).toMatchObject({failed: 1})
    expect(await isLocked(overall)).toEqual(false)

    expect(log.warn).toHaveBeenCalledTimes(1)
    expect(log.warn.mock.calls[0][0].message).toMatch('putRecords failure')
    expect(log.warn.mock.calls[0][1].Records.length).toEqual(1)
  })

  it('locks to the first digest downloaded', async () => {
    const calls = mockPutRecords(true, true)
    expect(await kinesis.putWithLock(redis, [overall])).toMatchObject({overall: 1})
    expect(await kinesis.putWithLock(redis, [{...overall, digest: '5679'}])).toMatchObject({overallDups: 1})
    expect(calls.length).toEqual(2)
    expect(calls[0].Records.length).toEqual(1)
    expect(JSON.parse(calls[0].Records[0].Data).isDuplicate).toBeUndefined()
    expect(calls[1].Records.length).toEqual(1)
    expect(JSON.parse(calls[1].Records[0].Data).isDuplicate).toEqual(true)
    expect(JSON.parse(calls[1].Records[0].Data).cause).toEqual('digestCache')
  })

  it('unfortunately does not unlock the digest for a failed putRecord', async () => {
    const calls = mockPutRecords(false, true)

    jest.spyOn(log, 'warn').mockImplementation(() => null)
    expect(await kinesis.putWithLock(redis, [segment])).toMatchObject({failed: 1})

    expect(await kinesis.putWithLock(redis, [{...segment, digest: '5679'}])).toMatchObject({segmentDups: 1})
    expect(calls.length).toEqual(2)
    expect(calls[1].Records.length).toEqual(1)
    expect(JSON.parse(calls[1].Records[0].Data).isDuplicate).toEqual(true)
    expect(JSON.parse(calls[1].Records[0].Data).cause).toEqual('digestCache')
  })

  it('splits into batches', async () => {
    const calls = mockPutRecords(true, true, true, true)
    const manyRecords = [
      overall, overall, overall,
      segment, overall, {...segment, le: '1235'},
      {...overall, le: '1235'}
    ]

    expect(await kinesis.putWithLock(redis, manyRecords, 3)).toMatchObject({
      overall: 2,
      overallDups: 0,
      segments: 2,
      segmentDups: 0,
      failed: 0
    })
    expect(calls.length).toEqual(3)

    // order of locks not guaranteed - but groupings are
    const keys = calls.map(c => c.Records.map(r => r.PartitionKey))
    const sorted = keys.sort((a, b) => JSON.stringify(a) < JSON.stringify(b) ? 1 : -1)

    expect(sorted[0]).toEqual(['1235'])
    expect(sorted[1]).toEqual(['1234'])
    expect(sorted[2]).toEqual(['1234', '1235'])
  })

})
