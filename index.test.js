const log = require('lambda-log')
const { handler } = require('./index')
const { BadEventError, RedisConnError } = require('./lib/errors')
const ByteRange = require('./lib/byte-range')
const decoder = require('./lib/kinesis-decoder')
const RedisBackup = require('./lib/redis-backup')
const s3 = require('./lib/s3')
const kinesis = require('./lib/kinesis')

jest.mock('./lib/kinesis-decoder')
jest.mock('./lib/s3')
jest.mock('./lib/kinesis')

describe('handler', () => {

  let redis
  beforeEach(() => {
    redis = new RedisBackup(process.env.REDIS_URL)
    jest.spyOn(log, 'info').mockImplementation(() => null)
    delete process.env.DEFAULT_BITRATE
    delete process.env.PERCENT_THRESHOLD
    delete process.env.SECONDS_THRESHOLD
  })

  afterEach(async () => {
    jest.restoreAllMocks()
    decoder.__clearBytes()
    s3.__clearArrangements()
    kinesis.__clearRecords()
    await redis.nuke('dtcounts:s3:itest*')
    await redis.nuke('dtcounts:bytes:itest*')
    await redis.nuke('dtcounts:imp:itest*')
    await redis.disconnect()
  })

  it('requires a redis url', async () => {
    const oldEnv = process.env.REDIS_URL
    try {
      jest.spyOn(log, 'error').mockImplementation(() => null)
      process.env.REDIS_URL = ''
      await handler()
      fail('should have gotten an error')
    } catch (err) {
      expect(err.name).toEqual('MissingEnvError')
      expect(err.message).toMatch(/REDIS_URL/i)
      expect(err.retryable).toEqual(true)
    } finally {
      process.env.REDIS_URL = oldEnv
    }
  })

  it('records whole downloads', async () => {
    s3.__addArrangement('itest-digest', {version:4, data: {t: 'oaoa', b: [703, 21643903, 22158271, 33348223, 33530815], a: [128, 1, 44100]}})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', time: 1, start: 0, end: 33530814})

    expect(await handler()).toMatchObject({overall: 1, segments: 2})
    expect(kinesis.__records.length).toEqual(3)
    expect(kinesis.__records[0]).toMatchObject({type: 'bytes'})
    expect(kinesis.__records[1]).toMatchObject({type: 'segmentbytes', segment: 1})
    expect(kinesis.__records[2]).toMatchObject({type: 'segmentbytes', segment: 3})
  })

  it('records empty downloads', async () => {
    s3.__addArrangement('itest-digest', {version: 4, data: {t: 'aao', b: [10, 20, 30, 40], a: [128, 1, 44100]}})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', time: 1, start: 0, end: 12})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', time: 1, start: 2, end: 10})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', time: 1, start: 33, end: 34})
    decoder.__addBytes({le: 'itest2', digest: 'itest-digest', time: 1, start: 22, end: 25})
    decoder.__addBytes({le: 'itest2', digest: 'itest-digest', time: 1, start: 0, end: 4})

    expect(await handler()).toMatchObject({overall: 0, segments: 0})
    expect(kinesis.__records.length).toEqual(0)
  })

  it('uses a seconds threshold', async () => {
    const bitrate = 0.080 // 10 bytes per second
    process.env.SECONDS_THRESHOLD = 10

    s3.__addArrangement('itest-digest', {version: 4, data: {t: 'o', b: [100, 300], a: [bitrate, 1, 44100]}})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', start: 0, end: 198, time: 99998})

    expect(await handler()).toMatchObject({overall: 0})
    expect(kinesis.__records.length).toEqual(0)

    decoder.__clearBytes()
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', start: 199, end: 199, time: 99999})

    expect(await handler()).toMatchObject({overall: 1})
    expect(kinesis.__records.length).toEqual(1)
    expect(kinesis.__records[0]).toEqual({
      type: 'bytes',
      timestamp: 99999,
      listenerEpisode: 'itest1',
      digest: 'itest-digest',
      bytes: 100,
      seconds: 10,
      percent: 0.5,
    })
  })

  it('uses a percentage threshold', async () => {
    const bitrate = 0.8 // 100 bytes per second
    process.env.PERCENT_THRESHOLD = 0.5

    s3.__addArrangement('itest-digest', {version: 4, data: {t: 'oa', b: [100, 400, 500], a: [bitrate, 1, 44100]}})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', start: 0, end: 248, time: 99990})

    expect(await handler()).toMatchObject({overall: 0})
    expect(kinesis.__records.length).toEqual(0)

    decoder.__clearBytes()
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', start: 399, end: 411, time: 99994})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', start: 100, end: 397, time: 99991})

    expect(await handler()).toMatchObject({overall: 1})
    expect(kinesis.__records.length).toEqual(1)
    expect(kinesis.__records[0]).toEqual({
      type: 'bytes',
      timestamp: 99994,
      listenerEpisode: 'itest1',
      digest: 'itest-digest',
      bytes: 311,
      seconds: 3.11,
      percent: 0.7775,
    })
  })

  it('defaults to requiring 100% of a short file to be downloaded', async () => {
    s3.__addArrangement('itest-digest', {version: 4, data: {t: 'o', b: [10, 20], a: [128, 1, 44100]}})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', start: 11, end: 19, time: 99999})

    expect(await handler()).toMatchObject({overall: 0})
    expect(kinesis.__records.length).toEqual(0)

    decoder.__clearBytes()
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', start: 10, end: 10, time: 99999})

    expect(await handler()).toMatchObject({overall: 1})
    expect(kinesis.__records.length).toEqual(1)
    expect(kinesis.__records[0]).toMatchObject({type: 'bytes'})
  })

  it('does not count segments until they are fully downloaded', async () => {
    s3.__addArrangement('itest-digest', {version: 4, data: {t: 'aao', b: [100, 200, 300, 4000], a: [128, 1, 44100]}})
    s3.__addArrangement('itest-digest2', {version: 4, data: {t: 'aao', b: [100, 200, 300, 4000], a: [128, 1, 44100]}})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', time: 1, start: 0, end: 198})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', time: 1, start: 200, end: 280})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', time: 1, start: 282, end: 300})

    expect(await handler()).toMatchObject({overall: 0, segments: 0})
    expect(kinesis.__records.length).toEqual(0)

    decoder.__clearBytes()
    decoder.__addBytes({le: 'itest2', digest: 'itest-digest', time: 1, start: 199, end: 199})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest2', time: 1, start: 199, end: 199})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', time: 1, start: 281, end: 281})

    expect(await handler()).toMatchObject({overall: 0, segments: 1})
    expect(kinesis.__records.length).toEqual(1)
    expect(kinesis.__records[0]).toEqual({
      type: 'segmentbytes',
      listenerEpisode: 'itest1',
      digest: 'itest-digest',
      segment: 1,
      timestamp: 1,
    })
  })

  it('does not count original segments', async () => {
    s3.__addArrangement('itest-digest', {version: 4, data: {t: 'aobisa?', b: [1, 2, 3, 4, 5, 6, 7, 8], a: [128, 1, 44100]}})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', time: 1, start: 0, end: 10})

    expect(await handler()).toMatchObject({overall: 1, segments: 6})
    expect(kinesis.__records.length).toEqual(7)
    expect(kinesis.__records[0]).toMatchObject({type: 'bytes'})
    expect(kinesis.__records[1]).toMatchObject({type: 'segmentbytes', segment: 0})
    expect(kinesis.__records[2]).toMatchObject({type: 'segmentbytes', segment: 2})
    expect(kinesis.__records[3]).toMatchObject({type: 'segmentbytes', segment: 3})
    expect(kinesis.__records[4]).toMatchObject({type: 'segmentbytes', segment: 4})
    expect(kinesis.__records[5]).toMatchObject({type: 'segmentbytes', segment: 5})
    expect(kinesis.__records[6]).toMatchObject({type: 'segmentbytes', segment: 6})
  })

  it('it warns on bad arrangements', async () => {
    jest.spyOn(log, 'warn').mockImplementation(() => null)

    s3.__addArrangement('itest-digest', {version: 4, data: {t: 'o', b: [10, 100], a: [128, 1, 44100]}})
    s3.__addArrangement('itest-digest2', {version: 2, data: {t: 'o', b: [10, 100]}})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', time: 1, start: 0, end: 100})
    decoder.__addBytes({le: 'itest2', digest: 'itest-digest2', time: 1, start: 0, end: 100})
    decoder.__addBytes({le: 'itest3', digest: 'foobar', time: 1, start: 0, end: 100})

    expect(await handler()).toMatchObject({overall: 1, segments: 0})
    expect(kinesis.__records.length).toEqual(1)
    expect(kinesis.__records[0]).toMatchObject({type: 'bytes', listenerEpisode: 'itest1'})
    expect(log.warn).toHaveBeenCalledTimes(2)
    const warns = log.warn.mock.calls.map(c => c[0].toString()).sort()
    expect(warns[0]).toMatch('ArrangementNoBytesError: Old itest-digest2')
    expect(warns[1]).toMatch('ArrangementNotFoundError: Missing foobar')
  })

  it('handles event parsing errors', async () => {
    const err = new BadEventError('Something bad')
    jest.spyOn(decoder, 'decodeEvent').mockRejectedValue(err)
    jest.spyOn(log, 'error').mockImplementation(() => null)
    expect(await handler()).toEqual(null)
    expect(log.error).toHaveBeenCalledTimes(1)
    expect(log.error.mock.calls[0][0].toString()).toMatch('BadEventError: Something bad')
  })

  it('throws and retries redis errors', async () => {
    const err = new RedisConnError('Something bad')
    jest.spyOn(ByteRange, 'load').mockRejectedValue(err)
    jest.spyOn(log, 'error').mockImplementation(() => null)
    s3.__addArrangement('itest-digest', {version: 4, data: {t: 'o', b: [10, 100], a: [128, 2, 44100]}})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', time: 1, start: 0, end: 100})
    try {
      await handler()
      fail('should have gotten an error')
    } catch (err) {
      expect(log.error).toHaveBeenCalledTimes(1)
      expect(log.error.mock.calls[0][0].toString()).toMatch('RedisConnError: Something bad')
    }
  })

  it('uses the default bitrate and warns on v3 arrangements', async () => {
    process.env.DEFAULT_BITRATE = 80 // 10 bytes per second
    process.env.SECONDS_THRESHOLD = 10

    s3.__addArrangement('itest-digest', {version: 3, data: {t: 'o', b: [100, 300]}})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', start: 0, end: 198, time: 99998})

    jest.spyOn(log, 'warn').mockImplementation()
    expect(await handler()).toMatchObject({overall: 0, segments: 0})
    expect(log.warn).toHaveBeenCalledTimes(1)
    expect(log.warn.mock.calls[0][0]).toEqual('Non v4 arrangement')
    expect(log.warn.mock.calls[0][1]).toEqual({digest: 'itest-digest'})

    decoder.__clearBytes()
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', start: 199, end: 199, time: 99999})

    expect(await handler()).toMatchObject({overall: 1, segments: 0})
    expect(kinesis.__records.length).toEqual(1)
    expect(kinesis.__records[0]).toMatchObject({type: 'bytes'})
    expect(log.warn).toHaveBeenCalledTimes(2)
    expect(log.warn.mock.calls[1][0]).toEqual('Non v4 arrangement')
    expect(log.warn.mock.calls[1][1]).toEqual({digest: 'itest-digest'})
  })

  it('throws a retryable error on kinesis put failure', async () => {
    jest.spyOn(kinesis, 'putWithLock').mockImplementation(async () => {
      return {failed: 1}
    })
    jest.spyOn(log, 'error').mockImplementation(() => null)

    s3.__addArrangement('itest-digest', {version: 4, data: {t: 'o', b: [10, 20], a: [128, 1, 44100]}})
    decoder.__addBytes({le: 'itest1', digest: 'itest-digest', start: 0, end: 19, time: 99999})
    try {
      await handler()
      fail('should have gotten an error')
    } catch (err) {
      expect(log.error).toHaveBeenCalledTimes(1)
      expect(log.error.mock.calls[0][0].toString()).toMatch('Failed to put 1')
    }
  })

})
