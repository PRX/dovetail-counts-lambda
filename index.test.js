const { handler } = require('./index')
const decoder = require('./lib/kinesis-decoder')
const Redis = require('./lib/redis')
const s3 = require('./lib/s3')

jest.mock('./lib/kinesis-decoder')
jest.mock('./lib/s3')

describe('handler', () => {

  const redis = new Redis()

  beforeEach(() => {
    delete process.env.DEFAULT_BITRATE
    delete process.env.SECONDS_THRESHOLD
  })

  afterEach(async () => {
    decoder.__clearBytes()
    s3.__clearArrangements()
    await redis.nuke('dtcounts:s3:test*')
    await redis.nuke('dtcounts:bytes:test*')
  })

  it('records empty downloads', async () => {
    s3.__addArrangement('test-digest', {version:3, data: {t:'aao', b:[10, 20, 30, 40]}})
    decoder.__addBytes({uuid: 'test1', digest: 'test-digest', start: 0, end: 12})
    decoder.__addBytes({uuid: 'test1', digest: 'test-digest', start: 2, end: 10})
    decoder.__addBytes({uuid: 'test1', digest: 'test-digest', start: 33, end: 34})
    decoder.__addBytes({uuid: 'test2', digest: 'test-digest', start: 22, end: 25})
    decoder.__addBytes({uuid: 'test2', digest: 'test-digest', start: 0, end: 4})

    const results = await handler()
    expect(results.test1.segments).toEqual([false, false, false])
    expect(results.test1.segmentBytes).toEqual([3, 0, 2])
    expect(results.test1.overall).toEqual(false)
    expect(results.test1.overallBytes).toEqual(5)
    expect(results.test2.segments).toEqual([false, false, false])
    expect(results.test2.segmentBytes).toEqual([0, 4, 0])
    expect(results.test2.overall).toEqual(false)
    expect(results.test2.overallBytes).toEqual(4)
  })

  it('uses a seconds threshold', async () => {
    process.env.DEFAULT_BITRATE = 80 // 10 bytes per second
    process.env.SECONDS_THRESHOLD = 10

    s3.__addArrangement('test-digest', {version:3, data: {t:'o', b:[100, 300]}})
    decoder.__addBytes({uuid: 'test1', digest: 'test-digest', start: 0, end: 198})

    const results1 = await handler()
    expect(results1.test1.segments).toEqual([false])
    expect(results1.test1.segmentBytes).toEqual([99])
    expect(results1.test1.overall).toEqual(false)
    expect(results1.test1.overallBytes).toEqual(99)

    decoder.__clearBytes()
    decoder.__addBytes({uuid: 'test1', digest: 'test-digest', start: 199, end: 199})

    const results2 = await handler()
    expect(results2.test1.segments).toEqual(['seconds'])
    expect(results2.test1.segmentBytes).toEqual([100])
    expect(results2.test1.overall).toEqual('seconds')
    expect(results2.test1.overallBytes).toEqual(100)
  })

  it('uses a percentage threshold', async () => {
    process.env.PERCENT_THRESHOLD = 0.5

    s3.__addArrangement('test-digest', {version:3, data: {t:'oa', b:[100, 400, 500]}})
    decoder.__addBytes({uuid: 'test1', digest: 'test-digest', start: 0, end: 248})

    const results1 = await handler()
    expect(results1.test1.segments).toEqual([false, false])
    expect(results1.test1.segmentBytes).toEqual([149, 0])
    expect(results1.test1.overall).toEqual(false)
    expect(results1.test1.overallBytes).toEqual(149)

    decoder.__clearBytes()
    decoder.__addBytes({uuid: 'test1', digest: 'test-digest', start: 399, end: 411})

    const results2 = await handler()
    expect(results2.test1.segments).toEqual(['percent', false])
    expect(results2.test1.segmentBytes).toEqual([150, 12])
    expect(results2.test1.overall).toEqual(false)
    expect(results2.test1.overallBytes).toEqual(162)

    decoder.__clearBytes()
    decoder.__addBytes({uuid: 'test1', digest: 'test-digest', start: 100, end: 400})

    const results3 = await handler()
    expect(results3.test1.segments).toEqual(['percent', false])
    expect(results3.test1.segmentBytes).toEqual([300, 12])
    expect(results3.test1.overall).toEqual('percent')
    expect(results3.test1.overallBytes).toEqual(312)
  })

})
