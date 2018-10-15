const log = require('lambda-log')
const { handler } = require('./index')
const { BadEventError, RedisConnError } = require('./lib/errors')
const ByteRange = require('./lib/byte-range')
const decoder = require('./lib/kinesis-decoder')
const Redis = require('./lib/redis')
const s3 = require('./lib/s3')
const kinesis = require('./lib/kinesis')

jest.mock('./lib/kinesis-decoder')
jest.mock('./lib/s3')
jest.mock('./lib/kinesis')

describe('handler', () => {

  let redis
  beforeEach(() => {
    redis = new Redis()
    jest.spyOn(log, 'info').mockImplementation(() => null)
    delete process.env.DEFAULT_BITRATE
    delete process.env.SECONDS_THRESHOLD
  })

  afterEach(async () => {
    jest.restoreAllMocks()
    decoder.__clearBytes()
    s3.__clearArrangements()
    kinesis.__clearRecords()
    await redis.nuke('dtcounts:s3:itest*')
    await redis.nuke('dtcounts:bytes:itest*')
    await redis.disconnect()
  })

  it('records empty downloads', async () => {
    s3.__addArrangement('itest-digest', {version:3, data: {t:'aao', b: [10, 20, 30, 40]}})
    decoder.__addBytes({uuid: 'itest1', digest: 'itest-digest', start: 0, end: 12})
    decoder.__addBytes({uuid: 'itest1', digest: 'itest-digest', start: 2, end: 10})
    decoder.__addBytes({uuid: 'itest1', digest: 'itest-digest', start: 33, end: 34})
    decoder.__addBytes({uuid: 'itest2', digest: 'itest-digest', start: 22, end: 25})
    decoder.__addBytes({uuid: 'itest2', digest: 'itest-digest', start: 0, end: 4})

    const results = await handler()
    expect(results.itest1.segments).toEqual([false, false, false])
    expect(results.itest1.segmentBytes).toEqual([3, 0, 2])
    expect(results.itest1.overall).toEqual(false)
    expect(results.itest1.overallBytes).toEqual(5)
    expect(results.itest2.segments).toEqual([false, false, false])
    expect(results.itest2.segmentBytes).toEqual([0, 4, 0])
    expect(results.itest2.overall).toEqual(false)
    expect(results.itest2.overallBytes).toEqual(4)
    expect(kinesis.__records.length).toEqual(0)
  })

  it('uses a seconds threshold', async () => {
    process.env.DEFAULT_BITRATE = 80 // 10 bytes per second
    process.env.SECONDS_THRESHOLD = 10

    s3.__addArrangement('itest-digest', {version:3, data: {t:'o', b: [100, 300]}})
    decoder.__addBytes({uuid: 'itest1', digest: 'itest-digest', start: 0, end: 198})

    const results1 = await handler()
    expect(results1.itest1.segments).toEqual([false])
    expect(results1.itest1.segmentBytes).toEqual([99])
    expect(results1.itest1.overall).toEqual(false)
    expect(results1.itest1.overallBytes).toEqual(99)
    expect(kinesis.__records.length).toEqual(0)

    decoder.__clearBytes()
    decoder.__addBytes({uuid: 'itest1', digest: 'itest-digest', start: 199, end: 199})

    const results2 = await handler()
    expect(results2.itest1.segments).toEqual(['seconds'])
    expect(results2.itest1.segmentBytes).toEqual([100])
    expect(results2.itest1.overall).toEqual('seconds')
    expect(results2.itest1.overallBytes).toEqual(100)
    expect(kinesis.__records.length).toEqual(2)
    expect(kinesis.__records[0]).toEqual({uuid: 'itest1', segment: 0, bytes: 100, seconds: 10, percent: 100 / 201})
    expect(kinesis.__records[1]).toEqual({uuid: 'itest1', bytes: 100, seconds: 10, percent: 100 / 201})
  })

  it('uses a percentage threshold', async () => {
    process.env.DEFAULT_BITRATE = 800 // 100 bytes per second
    process.env.PERCENT_THRESHOLD = 0.5

    s3.__addArrangement('itest-digest', {version:3, data: {t:'oa', b: [100, 400, 500]}})
    decoder.__addBytes({uuid: 'itest1', digest: 'itest-digest', start: 0, end: 248})

    const results1 = await handler()
    expect(results1.itest1.segments).toEqual([false, false])
    expect(results1.itest1.segmentBytes).toEqual([149, 0])
    expect(results1.itest1.overall).toEqual(false)
    expect(results1.itest1.overallBytes).toEqual(149)
    expect(kinesis.__records.length).toEqual(0)

    decoder.__clearBytes()
    decoder.__addBytes({uuid: 'itest1', digest: 'itest-digest', start: 399, end: 411})

    const results2 = await handler()
    expect(results2.itest1.segments).toEqual(['percent', false])
    expect(results2.itest1.segmentBytes).toEqual([150, 12])
    expect(results2.itest1.overall).toEqual(false)
    expect(results2.itest1.overallBytes).toEqual(162)
    expect(kinesis.__records.length).toEqual(1)
    expect(kinesis.__records[0]).toEqual({uuid: 'itest1', segment: 0, bytes: 150, seconds: 1.5, percent: 0.5})

    decoder.__clearBytes()
    decoder.__addBytes({uuid: 'itest1', digest: 'itest-digest', start: 100, end: 400})

    const results3 = await handler()
    expect(results3.itest1.segments).toEqual(['percent', false])
    expect(results3.itest1.segmentBytes).toEqual([300, 12])
    expect(results3.itest1.overall).toEqual('percent')
    expect(results3.itest1.overallBytes).toEqual(312)
    expect(kinesis.__records.length).toEqual(3)
    expect(kinesis.__records[1]).toEqual({uuid: 'itest1', segment: 0, bytes: 300, seconds: 3, percent: 1})
    expect(kinesis.__records[2]).toEqual({uuid: 'itest1', bytes: 312, seconds: 3.12, percent: 312 / 401})
  })

  it('it warns on bad arrangements', async () => {
    jest.spyOn(log, 'warn').mockImplementation(() => null)

    s3.__addArrangement('itest-digest', {version:3, data: {t:'o', b: [10, 100]}})
    s3.__addArrangement('itest-digest2', {version:2, data: {t:'o', b: [10, 100]}})
    decoder.__addBytes({uuid: 'itest1', digest: 'itest-digest', start: 0, end: 100})
    decoder.__addBytes({uuid: 'itest2', digest: 'itest-digest2', start: 0, end: 100})
    decoder.__addBytes({uuid: 'itest3', digest: 'foobar', start: 0, end: 100})

    const results = await handler()
    expect(results.itest1.overallBytes).toEqual(91)
    expect(log.warn).toHaveBeenCalledTimes(2)
    const warns = log.warn.mock.calls.map(c => c[0].toString()).sort()
    expect(warns[0]).toMatch('ArrangementNoBytesError: Old itest-digest2')
    expect(warns[1]).toMatch('ArrangementNotFoundError: Missing foobar')
  })

  it('handles event parsing errors', async () => {
    const err = new BadEventError('Something bad')
    jest.spyOn(decoder, 'decodeEvent').mockRejectedValue(err)
    jest.spyOn(log, 'error').mockImplementation(() => null)
    expect(await handler()).toEqual(false)
    expect(log.error).toHaveBeenCalledTimes(1)
    expect(log.error.mock.calls[0][0].toString()).toMatch('BadEventError: Something bad')
  })

  it('throws and retries redis errors', async () => {
    const err = new RedisConnError('Something bad')
    jest.spyOn(ByteRange, 'load').mockRejectedValue(err)
    jest.spyOn(log, 'error').mockImplementation(() => null)
    s3.__addArrangement('itest-digest', {version:3, data: {t:'o', b: [10, 100]}})
    decoder.__addBytes({uuid: 'itest1', digest: 'itest-digest', start: 0, end: 100})
    try {
      await handler()
      fail('should have gotten an error')
    } catch (err) {
      expect(log.error).toHaveBeenCalledTimes(1)
      expect(log.error.mock.calls[0][0].toString()).toMatch('RedisConnError: Something bad')
    }
  })

})
