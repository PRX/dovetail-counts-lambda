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

  // shorthand
  const time = 1537820000000

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
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', time, start: 0, end: 12})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', time, start: 2, end: 10})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', time, start: 33, end: 34})
    decoder.__addBytes({le: 'itest2', digest: 'itest-digest', time, start: 22, end: 25})
    decoder.__addBytes({le: 'itest2', digest: 'itest-digest', time, start: 0, end: 4})

    const results = await handler()
    expect(Object.keys(results)).toEqual(['itest1/itest-digest/2018-09-24', 'itest2/itest-digest/2018-09-24'])
    expect(results['itest1/itest-digest/2018-09-24'].segments).toEqual([false, false, false])
    expect(results['itest1/itest-digest/2018-09-24'].overall).toEqual(false)
    expect(results['itest1/itest-digest/2018-09-24'].overallBytes).toEqual(5)
    expect(results['itest2/itest-digest/2018-09-24'].segments).toEqual([false, false, false])
    expect(results['itest2/itest-digest/2018-09-24'].overall).toEqual(false)
    expect(results['itest2/itest-digest/2018-09-24'].overallBytes).toEqual(4)
    expect(kinesis.__records.length).toEqual(0)
  })

  it('uses a seconds threshold', async () => {
    process.env.DEFAULT_BITRATE = 80 // 10 bytes per second
    process.env.SECONDS_THRESHOLD = 10

    s3.__addArrangement('itest-digest', {version:3, data: {t:'o', b: [100, 300]}})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', start: 0, end: 198, time})

    const results1 = await handler()
    expect(results1['itest1/itest-digest/2018-09-24'].overall).toEqual(false)
    expect(results1['itest1/itest-digest/2018-09-24'].overallBytes).toEqual(99)
    expect(kinesis.__records.length).toEqual(0)

    decoder.__clearBytes()
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', start: 199, end: 199, time: time + 1})

    const results2 = await handler()
    expect(results2['itest1/itest-digest/2018-09-24'].overall).toEqual('seconds')
    expect(results2['itest1/itest-digest/2018-09-24'].overallBytes).toEqual(100)
    expect(kinesis.__records.length).toEqual(1)
    expect(kinesis.__records[0]).toEqual({
      time: time + 1,
      listenerEpisode: 'itest1',
      digest: 'itest-digest',
      bytes: 100,
      seconds: 10,
      percent: 100 / 201
    })
  })

  it('uses a percentage threshold', async () => {
    process.env.DEFAULT_BITRATE = 800 // 100 bytes per second
    process.env.PERCENT_THRESHOLD = 0.5

    s3.__addArrangement('itest-digest', {version:3, data: {t:'oa', b: [100, 400, 500]}})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', start: 0, end: 248, time})

    const results1 = await handler()
    expect(results1['itest1/itest-digest/2018-09-24'].overall).toEqual(false)
    expect(results1['itest1/itest-digest/2018-09-24'].overallBytes).toEqual(149)
    expect(kinesis.__records.length).toEqual(0)

    decoder.__clearBytes()
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', start: 399, end: 411, time: time + 3})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', start: 100, end: 397, time})

    const results2 = await handler()
    expect(results2['itest1/itest-digest/2018-09-24'].overall).toEqual('percent')
    expect(results2['itest1/itest-digest/2018-09-24'].overallBytes).toEqual(311)
    expect(kinesis.__records.length).toEqual(1)
    expect(kinesis.__records[0]).toEqual({
      time: time + 3,
      listenerEpisode: 'itest1',
      digest: 'itest-digest',
      bytes: 311,
      seconds: 3.11,
      percent: 311 / 401
    })
  })

  it('does not count segments until they are fully downloaded', async () => {
    s3.__addArrangement('itest-digest', {version:3, data: {t:'aao', b: [100, 200, 300, 4000]}})
    s3.__addArrangement('itest-digest2', {version:3, data: {t:'aao', b: [100, 200, 300, 4000]}})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', time, start: 0, end: 198})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', time, start: 200, end: 280})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', time, start: 282, end: 300})

    const results1 = await handler()
    expect(results1['itest1/itest-digest/2018-09-24'].segments).toEqual([false, false, false])
    expect(kinesis.__records.length).toEqual(0)

    decoder.__clearBytes()
    decoder.__addBytes({le: 'itest2', digest: 'itest-digest', time, start: 199, end: 199})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest2', time, start: 199, end: 199})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', time, start: 281, end: 281})

    const results2 = await handler()
    expect(results2['itest1/itest-digest/2018-09-24'].segments).toEqual([false, true, false])
    expect(results2['itest1/itest-digest2/2018-09-24'].segments).toEqual([false, false, false])
    expect(results2['itest2/itest-digest/2018-09-24'].segments).toEqual([false, false, false])
    expect(kinesis.__records.length).toEqual(1)
    expect(kinesis.__records[0]).toEqual({listenerEpisode: 'itest1', digest: 'itest-digest', time, segment: 1})
  })

  it('it warns on bad arrangements', async () => {
    jest.spyOn(log, 'warn').mockImplementation(() => null)

    s3.__addArrangement('itest-digest', {version:3, data: {t:'o', b: [10, 100]}})
    s3.__addArrangement('itest-digest2', {version:2, data: {t:'o', b: [10, 100]}})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', time, start: 0, end: 100})
    decoder.__addBytes({le: 'itest2', digest: 'itest-digest2', time, start: 0, end: 100})
    decoder.__addBytes({le: 'itest3', digest: 'foobar', time, start: 0, end: 100})

    const results = await handler()
    expect(results['itest1/itest-digest/2018-09-24'].overallBytes).toEqual(91)
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
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', time, start: 0, end: 100})
    try {
      await handler()
      fail('should have gotten an error')
    } catch (err) {
      expect(log.error).toHaveBeenCalledTimes(1)
      expect(log.error.mock.calls[0][0].toString()).toMatch('RedisConnError: Something bad')
    }
  })

})
