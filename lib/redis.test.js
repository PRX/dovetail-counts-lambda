const Redis = require('./redis')

describe('redis', () => {

  const TEST_KEY = 'dovetail-counts-lambda.redis-test'

  let redis
  beforeEach(() => redis = new Redis())
  afterEach(async () => {
    await redis.del(TEST_KEY)
    await redis.disconnect()
  })

  it('connects and disconnects', async () => {
    expect(redis.connected).toEqual(false)
    const connect = redis.connect()
    expect(redis.connected).toEqual(false)
    await connect
    expect(redis.connected).toEqual(true)

    const disconnect = redis.disconnect()
    expect(redis.connected).toEqual(true)
    await disconnect
    expect(redis.connected).toEqual(false)
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
