const Redis = require('./redis')

describe('redis', () => {

  const TEST_KEY = 'dovetail-counts-lambda.redis-test'

  let redis, redisUrl
  beforeEach(() => {
    redisUrl = process.env.REDIS_URL
    redis = new Redis()
  })
  afterEach(async () => {
    await redis.del(TEST_KEY)
    await redis.disconnect()
    process.env.REDIS_URL = redisUrl
  })

  it('requires a redis url', async () => {
    delete process.env.REDIS_URL
    try {
      new Redis()
      fail('should have gotten an error')
    } catch (err) {
      expect(err.name).toEqual('MissingEnvError')
      expect(err.message).toMatch(/REDIS_URL/i)
    }
  })

  it('throws connection errors', async () => {
    process.env.REDIS_URL = 'redis://does-not-exist'
    try {
      const r2 = new Redis(0)
      await r2.get('anything')
    } catch (err) {
      expect(err.name).toEqual('RedisConnError')
      expect(err.message).toMatch(/connection is closed/i)
    }
  })

  it('connects and disconnects', async () => {
    const r2 = new Redis()
    expect(r2.connected).toEqual(false)
    const connect = r2.get('anything')
    expect(r2.connected).toEqual(false)
    await connect
    expect(r2.connected).toEqual(true)

    await r2.disconnect()
    expect(r2.connected).toEqual(false)
  })

  it('does basic redis operations', async () => {
    expect(redis.connected).toEqual(false)
    expect(await redis.get(TEST_KEY)).toEqual(null)
    expect(await redis.set(TEST_KEY, 'some-val')).toEqual('OK')
    expect(await redis.get(TEST_KEY)).toEqual('some-val')
    expect(await redis.del(TEST_KEY)).toEqual(1)
    expect(await redis.get(TEST_KEY)).toEqual(null)
    expect(redis.connected).toEqual(true)
  })

  it('sets with a ttl', async () => {
    expect(await redis.setex(TEST_KEY, 99, 'some-val')).toEqual('OK')
    expect(await redis.ttl(TEST_KEY)).toEqual(99)
  })

  it('gets valid json or null', async () => {
    expect(await redis.getJson(TEST_KEY)).toEqual(null)
    await redis.set(TEST_KEY, 'some-val')
    expect(await redis.getJson(TEST_KEY)).toEqual(null)
    await redis.set(TEST_KEY, '{"some":"json"}')
    expect(await redis.getJson(TEST_KEY)).toEqual({some: 'json'})
  })

  it('pushes strings onto a list', async () => {
    expect(await redis.get(TEST_KEY)).toEqual(null)
    expect(await redis.push(TEST_KEY, 'val1')).toEqual('val1')
    expect(await redis.push(TEST_KEY, '')).toEqual('val1')
    expect(await redis.push(TEST_KEY, [])).toEqual('val1')
    expect(await redis.get(TEST_KEY)).toEqual('val1')
    expect(await redis.push(TEST_KEY, 'val2')).toEqual('val1,val2')
    expect(await redis.get(TEST_KEY)).toEqual('val1,val2')
    expect(await redis.push(TEST_KEY, 'val3,val4')).toEqual('val1,val2,val3,val4')
    expect(await redis.get(TEST_KEY)).toEqual('val1,val2,val3,val4')
    expect(await redis.push(TEST_KEY, [5, 6])).toEqual('val1,val2,val3,val4,5,6')
    expect(await redis.get(TEST_KEY)).toEqual('val1,val2,val3,val4,5,6')
  })

  it('pushes with a ttl', async () => {
    expect(await redis.ttl(TEST_KEY)).toEqual(-2)
    await redis.push(TEST_KEY, 'val1')
    expect(await redis.ttl(TEST_KEY)).toEqual(-1)
    await redis.push(TEST_KEY, 'val2', 100)
    expect(await redis.ttl(TEST_KEY)).toEqual(100)
  })

})
