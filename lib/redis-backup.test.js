const log = require('lambda-log')
const RedisBackup = require('./redis-backup')
const Redis = require('./redis')

describe('redis-backup', () => {

  const TEST_KEY = 'dovetail-counts-lambda.redis-backup-test'

  let db0, db1, redis, redisUrl, redisBackupUrl
  beforeEach(() => {
    process.env.REDIS_BACKUP_URL = 'redis://127.0.0.1:6379/1'
    db0 = new Redis(process.env.REDIS_URL)
    db1 = new Redis(process.env.REDIS_BACKUP_URL)
    redisUrl = process.env.REDIS_URL
    redisBackupUrl = process.env.REDIS_BACKUP_URL
    redis = new RedisBackup()
  })
  afterEach(async () => {
    await db0.del(TEST_KEY)
    await db0.disconnect()
    await db1.del(TEST_KEY)
    await db1.disconnect()
    await redis.disconnect()
    process.env.REDIS_URL = redisUrl
    process.env.REDIS_BACKUP_URL = redisBackupUrl
  })

  it('requires a redis url', async () => {
    try {
      process.env.REDIS_URL = ''
      new RedisBackup()
      fail('should have gotten an error')
    } catch (err) {
      expect(err.name).toEqual('MissingEnvError')
      expect(err.message).toMatch(/REDIS_URL/i)
    }
  })

  it('works without a backup url', async () => {
    process.env.REDIS_BACKUP_URL = ''
    redis = new RedisBackup()
    await redis.set(TEST_KEY, 'something')

    expect(await db0.get(TEST_KEY)).toEqual('something')
    expect(await db1.get(TEST_KEY)).toEqual(null)
  })

  it('logs but does not throw backup errors', async () => {
    await db0.cmd('hset', TEST_KEY, 'fld', 'string-value')
    await db1.set(TEST_KEY, 'string-value')

    jest.spyOn(log, 'warn').mockImplementation(() => null)
    await redis.cmd('hset', TEST_KEY, 'fld', 'something-else')

    expect(log.warn).toHaveBeenCalledTimes(1)
    expect(log.warn.mock.calls[0][1]).toEqual('Backup redis failed')
    expect(log.warn.mock.calls[0][0].err.message).toMatch(/WRONGTYPE/)

    expect(await redis.cmd('hget', TEST_KEY, 'fld')).toEqual('something-else')
    expect(await db0.cmd('type', TEST_KEY)).toEqual('hash')
    expect(await db1.cmd('type', TEST_KEY)).toEqual('string')
  })

  it('gets values from the primary url', async () => {
    await db0.set(TEST_KEY, 'val0')
    await db1.set(TEST_KEY, 'val1')

    expect(await redis.get(TEST_KEY)).toEqual('val0')
  })

  it('gets ttls from the primary url', async () => {
    await db0.setex(TEST_KEY, 111, 'val')
    await db1.setex(TEST_KEY, 22, 'val')

    expect(await redis.ttl(TEST_KEY)).toEqual(111)
  })

  it('sets values to both urls', async () => {
    await redis.set(TEST_KEY, 'something')

    expect(await db0.get(TEST_KEY)).toEqual('something')
    expect(await db1.get(TEST_KEY)).toEqual('something')
  })

  it('lockValues on both urls', async () => {
    expect(await redis.lockValue(TEST_KEY, 'val1')).toEqual(true)
    expect(await redis.lockValue(TEST_KEY, 'val2')).toEqual(false)

    expect(await db0.get(TEST_KEY)).toEqual('val1')
    expect(await db1.get(TEST_KEY)).toEqual('val1')
  })

  it('locks on both urls', async () => {
    expect(await redis.lock(TEST_KEY, 'some-fld', 99)).toEqual(true)
    expect(await redis.lock(TEST_KEY, 'some-fld', 55)).toEqual(false)

    expect(await db0.ttl(TEST_KEY)).toEqual(99)
    expect(await db1.ttl(TEST_KEY)).toEqual(99)
  })

  it('pushes to both urls', async () => {
    expect(await redis.get(TEST_KEY)).toEqual(null)
    expect(await redis.push(TEST_KEY, 'val1')).toEqual('val1')
    expect(await redis.push(TEST_KEY, 'val2')).toEqual('val1,val2')
  })

})
