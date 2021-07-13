const log = require('lambda-log')
const Arrangement = require('./arrangement')
const RedisBackup = require('./redis-backup')
const dynamo = require('./dynamo')
const s3 = require('./s3')

jest.mock('./dynamo')
jest.mock('./s3')

describe('arrangement', () => {
  const DIGEST = 'arrangement.test'
  const KEY = `dtcounts:s3:${DIGEST}`

  let redis, mockData, warns
  beforeEach(() => {
    redis = new RedisBackup(process.env.REDIS_URL)
    mockData = {
      version: 4,
      data: {
        f: ['http://f1.mp3', 'http://f2.mp3', 'http://f3.mp3'],
        t: 'aao',
        b: [123, 456, 789, 101112],
        a: [192, 1, 44100],
      },
    }
    warns = []
    jest.spyOn(log, 'warn').mockImplementation((...args) => warns.push(args))
    delete process.env.DEFAULT_BITRATE
  })
  afterEach(async () => {
    dynamo.__clearArrangements()
    s3.__clearArrangements()
    await redis.del(KEY)
    await redis.disconnect()
    jest.restoreAllMocks()
  })

  it('gets json from dynamodb and uploads to redis', async () => {
    dynamo.__addArrangement(DIGEST, mockData)
    expect(await redis.get(KEY)).toBeNull()

    const arr = await Arrangement.load(DIGEST, redis)
    expect(arr.version).toEqual(4)
    expect(arr.types).toEqual('aao')
    expect(arr.bytes).toEqual([123, 456, 789, 101112])
    expect(arr.analysis).toEqual({ f: 'mp3', b: 192, c: 1, s: 44100 })
    expect(arr.bitrate).toEqual(192000)

    const json = await redis.get(KEY)
    expect(json).not.toBeNull()
    expect(json).toEqual(arr.encode())
  })

  it('gets json from s3 and uploads to redis', async () => {
    s3.__addArrangement(DIGEST, mockData)
    expect(await redis.get(KEY)).toBeNull()

    const arr = await Arrangement.load(DIGEST, redis)
    expect(arr.version).toEqual(4)
    expect(arr.types).toEqual('aao')
    expect(arr.bytes).toEqual([123, 456, 789, 101112])
    expect(arr.analysis).toEqual({ f: 'mp3', b: 192, c: 1, s: 44100 })
    expect(arr.bitrate).toEqual(192000)

    const json = await redis.get(KEY)
    expect(json).not.toBeNull()
    expect(json).toEqual(arr.encode())
  })

  it('memoizes requests for the same arrangement', async () => {
    s3.__addArrangement(DIGEST, mockData)
    const arrs = await Promise.all([
      Arrangement.load(DIGEST, redis),
      Arrangement.load(DIGEST, redis),
      Arrangement.load(DIGEST, redis),
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
    }
  })

  it('loads and upgrades v3 arrangements', async () => {
    mockData.version = 3
    delete mockData.data.a
    s3.__addArrangement(DIGEST, mockData)
    expect(await redis.get(KEY)).toBeNull()

    const arr = await Arrangement.load(DIGEST, redis)
    expect(arr.version).toEqual(3)
    expect(arr.types).toEqual('aao')
    expect(arr.bytes).toEqual([123, 456, 789, 101112])
    expect(arr.analysis).toBeNull()
    expect(arr.bitrate).toEqual(128000)

    expect(warns.length).toEqual(1)
    expect(warns[0][0]).toEqual('Non v4 arrangement')
    expect(warns[0][1]).toEqual({ digest: DIGEST })
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
    expect(arr.version).toEqual(4)
  })

  it('calculates mp3 bitrates', () => {
    let arr = new Arrangement(DIGEST, { version: 3, data: { t: 'o', b: [1, 2] } })
    expect(arr.bitrate).toEqual(128000)
    expect(warns.length).toEqual(1)

    process.env.DEFAULT_BITRATE = '129000'
    arr = new Arrangement(DIGEST, { version: 3, data: { t: 'o', b: [1, 2] } })
    expect(arr.bitrate).toEqual(129000)
    expect(warns.length).toEqual(2)

    arr = new Arrangement(DIGEST, { version: 4, data: { t: 'o', b: [1, 2], a: [130, 1, 44100] } })
    expect(arr.bitrate).toEqual(130000)
    expect(warns.length).toEqual(2)

    arr = new Arrangement(DIGEST, {
      version: 4,
      data: { t: 'o', b: [1, 2], a: [131000, 1, 44100] },
    })
    expect(arr.bitrate).toEqual(131000)
    expect(warns.length).toEqual(2)

    arr = new Arrangement(DIGEST, {
      version: 4,
      data: { t: 'o', b: [1, 2], a: { f: 'mp3', b: 130, c: 1, s: 44100 } },
    })
    expect(arr.bitrate).toEqual(130000)
    expect(warns.length).toEqual(2)
  })

  it('calculates wav bitrates', () => {
    let arr = new Arrangement(DIGEST, {
      version: 4,
      data: { t: 'o', b: [1, 2], a: { f: 'wav', b: 16, c: 2, s: 44100 } },
    })
    expect(arr.bitrate).toEqual(1411200)
    expect(warns.length).toEqual(0)
  })

  it('guesses flac bitrates', () => {
    let arr = new Arrangement(DIGEST, {
      version: 4,
      data: { t: 'o', b: [1, 2], a: { f: 'flac', b: 16, c: 2, s: 44100 } },
    })
    expect(arr.bitrate).toEqual(705600)
    expect(warns.length).toEqual(0)
  })

  it('calculates segment ranges', () => {
    const arr = new Arrangement(DIGEST, { version: 3, data: { t: 'ooo', b: [10, 20, 30, 40] } })
    expect(arr.segments.length).toEqual(3)
    expect(arr.segments[0]).toEqual([10, 19])
    expect(arr.segments[1]).toEqual([20, 29])
    expect(arr.segments[2]).toEqual([30, 39])
  })

  it('calculates segment sizes', () => {
    const arr = new Arrangement(DIGEST, { version: 3, data: { t: 'ooo', b: [10, 20, 30, 40] } })
    expect(arr.segments.length).toEqual(3)
    expect(arr.segmentSize(0)).toEqual(10)
    expect(arr.segmentSize(1)).toEqual(10)
    expect(arr.segmentSize(2)).toEqual(10)
    expect(arr.segmentSize()).toEqual(30)
  })

  it('converts bytes to seconds', () => {
    const arr = new Arrangement(DIGEST, { version: 3, data: { t: 'ooo', b: [10, 20, 30, 40] } })

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
    const arr = new Arrangement(DIGEST, { version: 3, data: { t: 'oo', b: [10, 20, 40] } })
    expect(arr.bytesToPercent(10, 0)).toEqual(1)
    expect(arr.bytesToPercent(2, 0)).toEqual(0.2)
    expect(arr.bytesToPercent(10, 1)).toEqual(0.5)
    expect(arr.bytesToPercent(15, 1)).toEqual(0.75)
    expect(arr.bytesToPercent(15)).toEqual(0.5)
    expect(arr.bytesToPercent(12)).toEqual(0.4)
    expect(arr.bytesToPercent(3)).toEqual(0.1)
  })

  it('only logs non-original type segments', () => {
    const arr = new Arrangement(DIGEST, {
      version: 3,
      data: { t: 'aobisa?', b: [1, 2, 3, 4, 5, 6, 7, 8] },
    })
    expect(arr.isLoggable(0)).toEqual(true)
    expect(arr.isLoggable(1)).toEqual(false)
    expect(arr.isLoggable(2)).toEqual(true)
    expect(arr.isLoggable(3)).toEqual(true)
    expect(arr.isLoggable(4)).toEqual(true)
    expect(arr.isLoggable(5)).toEqual(true)
    expect(arr.isLoggable(6)).toEqual(true)
    expect(arr.isLoggable(99)).toEqual(false)
  })
})
