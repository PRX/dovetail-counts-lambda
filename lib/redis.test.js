const log = require('lambda-log')
const Redis = require('./redis')

describe('redis', () => {

  const TEST_KEY = 'dovetail-counts-lambda.redis-test'

  let redis
  beforeEach(() => {
    redis = new Redis(process.env.REDIS_URL)
  })
  afterEach(async () => {
    await redis.del(TEST_KEY)
    await redis.disconnect()
  })

  it('throws connection errors', async () => {
    jest.spyOn(log, 'error').mockImplementation(() => null)
    try {
      const r2 = new Redis('redis://foo.bar.biz.gov', 0)
      await r2.get('anything')
    } catch (err) {
      expect(err.name).toEqual('RedisConnError')
      expect(err.message).toMatch(/connection is closed/i)
      expect(log.error).toHaveBeenCalledTimes(1)
      expect(log.error.mock.calls[0][0].toString()).toMatch('Error: getaddrinfo ENOTFOUND')
    }
  })

  it('connects and disconnects', async () => {
    const r2 = new Redis(process.env.REDIS_URL)
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

  it('locks with a value', async () => {
    expect(await redis.lockValue(TEST_KEY, 'val1')).toEqual(true)
    expect(await redis.lockValue(TEST_KEY, 'val2')).toEqual(false)
    expect(await redis.lockValue(TEST_KEY, 'val1')).toEqual(true)
    expect(await redis.ttl(TEST_KEY)).toEqual(-1)
  });

  it('locks with a value and ttl', async () => {
    expect(await redis.lockValue(TEST_KEY, 'val1', 99)).toEqual(true)
    expect(await redis.lockValue(TEST_KEY, 'val1', 33)).toEqual(true)
    expect(await redis.ttl(TEST_KEY)).toEqual(99)
  });

  it('locks a key/field hash', async () => {
    expect(await redis.lock(TEST_KEY, 'some-fld', 99)).toEqual(true)
    expect(await redis.ttl(TEST_KEY)).toEqual(99)
    expect(await redis.lock(TEST_KEY, 'some-fld', 55)).toEqual(false)
    expect(await redis.ttl(TEST_KEY)).toEqual(99)
    expect(await redis.lock(TEST_KEY, 'some-fld2', 33)).toEqual(true)
    expect(await redis.ttl(TEST_KEY)).toEqual(33)
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
