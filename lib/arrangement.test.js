const Arrangement = require('./arrangement')
const Redis = require('./redis')
const s3 = require('./s3')

jest.mock('./s3')

describe('arrangement', () => {

  const DIGEST = 'test-digest'
  const KEY = `dtcounts:s3:${DIGEST}`

  let redis, mockData
  beforeEach(() => {
    redis = new Redis()
    mockData = {version:3,data:{f:['http://f1.mp3','http://f2.mp3','http://f3.mp3'],t:'aao',b:[123,456,789,101112]}}
  })
  afterEach(async () => {
    s3.__clearArrangements()
    await redis.del(KEY)
    await redis.disconnect()
  })

  it.only('gets json from s3 and uploads to redis', async () => {
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
    }
  })

  it('throws an error for missing arrangements', async () => {
    try {
      await Arrangement.load(DIGEST, redis)
      fail('should have gotten an error')
    } catch (err) {
      expect(err.name).toEqual('ArrangementNotFoundError')
      expect(err.message).toEqual(`Missing ${DIGEST}`)
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
  })

})
