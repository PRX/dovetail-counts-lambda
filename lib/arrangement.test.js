const Arrangement = require('./arrangement')
const Redis = require('./redis')
const s3 = require('./s3')
const kinesis = require('./kinesis')

jest.mock('./s3')
jest.mock('./kinesis')

describe('arrangement', () => {

  const DIGEST = 'arrangement.test'
  const KEY = `dtcounts:s3:${DIGEST}`

  let redis, mockData
  beforeEach(() => {
    redis = new Redis()
    mockData = {version:3,data:{f:['http://f1.mp3','http://f2.mp3','http://f3.mp3'],t:'aao',b:[123,456,789,101112]}}
    delete process.env.DEFAULT_BITRATE
  })
  afterEach(async () => {
    s3.__clearArrangements()
    kinesis.__clearRecords()
    await redis.del(KEY)
    await redis.disconnect()
  })

  it('gets json from s3 and uploads to redis', async () => {
    s3.__addArrangement(DIGEST, mockData)
    expect(await redis.get(KEY)).toBeNull()

    const arr = await Arrangement.load(DIGEST, redis)
    expect(arr.version).toEqual(3)
    expect(arr.types).toEqual('aao')
    expect(arr.bytes).toEqual([123, 456, 789, 101112])

    const json = await redis.get(KEY)
    expect(json).not.toBeNull()
    expect(json).toEqual(arr.encode())
  })

  it('memoizes requests for the same arrangement', async () => {
    s3.__addArrangement(DIGEST, mockData)
    const arrs = await Promise.all([
      Arrangement.load(DIGEST, redis),
      Arrangement.load(DIGEST, redis),
      Arrangement.load(DIGEST, redis)
    ])
    expect(arrs[0] === arrs[1]).toEqual(true)
    expect(arrs[1] === arrs[2]).toEqual(true)
  })

  it('throws an error loading v2 arrangements', async () => {
    mockData.version = 2
    s3.__addArrangement(DIGEST, mockData)
    expect(await redis.get(KEY)).toBeNull()

    try {
      await Arrangement.load(DIGEST, redis)
      fail('should have gotten an error')
    } catch (err) {
      expect(err.name).toEqual('ArrangementNoBytesError')
      expect(err.message).toEqual(`Old ${DIGEST}`)
      expect(kinesis.__getDigests()).toEqual([DIGEST])
    }
  })

  it('throws an error for missing arrangements', async () => {
    try {
      await Arrangement.load(DIGEST, redis)
      fail('should have gotten an error')
    } catch (err) {
      expect(err.name).toEqual('ArrangementNotFoundError')
      expect(err.message).toEqual(`Missing ${DIGEST}`)
      expect(kinesis.__getDigests()).toEqual([DIGEST])
    }
  })

  it('loads directly from redis', async () => {
    await redis.set(KEY, JSON.stringify(mockData))
    const arr = await Arrangement.load(DIGEST, redis)
    expect(arr.version).toEqual(3)
  })

  it('calculates segment ranges', () => {
    const arr = new Arrangement(DIGEST, {version: 3, data: {t: 'ooo', b: [10, 20, 30, 40]}})
    expect(arr.segments.length).toEqual(3)
    expect(arr.segments[0]).toEqual([10, 19])
    expect(arr.segments[1]).toEqual([20, 29])
    expect(arr.segments[2]).toEqual([30, 40])
  })

  it('calculates segment sizes', () => {
    const arr = new Arrangement(DIGEST, {version: 3, data: {t: 'ooo', b: [10, 20, 30, 40]}})
    expect(arr.segments.length).toEqual(3)
    expect(arr.segmentSize(0)).toEqual(10)
    expect(arr.segmentSize(1)).toEqual(10)
    expect(arr.segmentSize(2)).toEqual(11)
    expect(arr.segmentSize()).toEqual(31)
  })

  it('converts bytes to seconds', () => {
    const arr = new Arrangement(DIGEST, {version: 3, data: {t: 'ooo', b: [10, 20, 30, 40]}})

    process.env.DEFAULT_BITRATE = 128000
    expect(arr.bytesToSeconds(16000)).toEqual(1)
    expect(arr.bytesToSeconds(128000)).toEqual(8)
    expect(arr.bytesToSeconds(100000)).toEqual(6.25)

    process.env.DEFAULT_BITRATE = 64000
    expect(arr.bytesToSeconds(16000)).toEqual(2)
    expect(arr.bytesToSeconds(128000)).toEqual(16)
    expect(arr.bytesToSeconds(100000)).toEqual(12.5)
  })

  it('converts bytes to percentages', () => {
    const arr = new Arrangement(DIGEST, {version: 3, data: {t: 'oo', b: [10, 20, 39]}})
    expect(arr.bytesToPercent(10, 0)).toEqual(1)
    expect(arr.bytesToPercent(2, 0)).toEqual(0.2)
    expect(arr.bytesToPercent(10, 1)).toEqual(0.5)
    expect(arr.bytesToPercent(15, 1)).toEqual(0.75)
    expect(arr.bytesToPercent(15)).toEqual(0.5)
    expect(arr.bytesToPercent(12)).toEqual(0.4)
    expect(arr.bytesToPercent(3)).toEqual(0.1)
  })

})
