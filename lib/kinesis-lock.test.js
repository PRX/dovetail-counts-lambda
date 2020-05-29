const log = require('lambda-log')
const lock = require('./kinesis-lock')
const RedisBackup = require('./redis-backup')

describe('kinesis-lock', () => {

  let redis, overall, segment
  beforeEach(() => {
    redis = new RedisBackup(process.env.REDIS_URL)
    overall = {listenerEpisode: '9999', digest: '5678', timestamp: 9, bytes: 10, seconds: 12, percent: 0.4}
    segment = {...overall, segment: 2}
  })

  afterEach(async () => {
    await redis.nuke('dtcounts:imp:9999:*')
    await redis.nuke('dtcounts:imp:8888:*')
    await redis.disconnect()
  })

  it('ignores nulls', async () => {
    expect(await lock.lock(redis, null)).toEqual(null)
    expect(await lock.unlock(redis, null)).toEqual(null)
    expect(await lock.lockDigest(redis, null)).toEqual(null)
  })

  it('locks the overall download', async () => {
    expect(await lock.lock(redis, overall)).toEqual(overall)
    expect(await lock.lock(redis, overall)).toEqual(null)

    const changed = {...overall, listenerEpisode: '8888'}
    expect(await lock.lock(redis, changed)).toEqual(changed)
    expect(await lock.lock(redis, changed)).toEqual(null)

    expect(await redis.cmd('hgetall', 'dtcounts:imp:9999:1970-01-01:5678')).toEqual({all: ''})
    expect(await redis.cmd('hgetall', 'dtcounts:imp:8888:1970-01-01:5678')).toEqual({all: ''})
  })

  it('locks segment impressions', async () => {
    expect(await lock.lock(redis, segment)).toEqual(segment)
    expect(await lock.lock(redis, segment)).toEqual(null)

    const changed = {...segment, segment: 3}
    expect(await lock.lock(redis, changed)).toEqual(changed)
    expect(await lock.lock(redis, changed)).toEqual(null)

    expect(await redis.cmd('hgetall', 'dtcounts:imp:9999:1970-01-01:5678')).toEqual({2: '', 3: ''})
  })

  it('unlocks overall downloads', async () => {
    expect(await lock.lock(redis, overall)).toEqual(overall)
    expect(await redis.cmd('hgetall', 'dtcounts:imp:9999:1970-01-01:5678')).toEqual({all: ''})
    expect(await lock.unlock(redis, overall)).toEqual(true)
    expect(await redis.cmd('hgetall', 'dtcounts:imp:9999:1970-01-01:5678')).toEqual({})
  })

  it('unlocks segment impressions', async () => {
    expect(await lock.lock(redis, segment)).toEqual(segment)
    expect(await redis.cmd('hgetall', 'dtcounts:imp:9999:1970-01-01:5678')).toEqual({2: ''})
    expect(await lock.unlock(redis, segment)).toEqual(true)
    expect(await redis.cmd('hgetall', 'dtcounts:imp:9999:1970-01-01:5678')).toEqual({})
  })

  it('knows if something is locked', async () => {
    expect(await lock.isLocked(redis, overall)).toEqual(false)
    expect(await lock.isLocked(redis, segment)).toEqual(false)

    await lock.lock(redis, overall)
    await lock.lock(redis, segment)

    expect(await lock.isLocked(redis, overall)).toEqual(true)
    expect(await lock.isLocked(redis, segment)).toEqual(true)
  })

  it('ignores unlock errors', async () => {
    await redis.set('dtcounts:imp:9999:1970-01-01:5678', 'string-value')
    jest.spyOn(log, 'warn').mockImplementation(() => null)

    expect(await lock.unlock(redis, segment)).toEqual(false)
    expect(log.warn).toHaveBeenCalledTimes(1)
    expect(log.warn.mock.calls[0][0].message).toMatch(/WRONGTYPE/)
    expect(log.warn.mock.calls[0][1]).toMatchObject({
      key: 'dtcounts:imp:9999:1970-01-01:5678',
      fld: 2,
      msg: 'Kinesis unlock failed'
    })
  })

  it('locks overall download digests', async () => {
    expect(await lock.lockDigest(redis, overall)).toEqual(overall)
    expect(await lock.lockDigest(redis, overall)).toEqual(overall)
    expect(await redis.get('dtcounts:imp:9999:1970-01-01:digest')).toEqual('5678')

    const changed = {...overall, digest: '5679'}
    expect(await lock.lockDigest(redis, changed)).toEqual({...changed, isDuplicate: true, cause: 'digestCache'})
    expect(await redis.get('dtcounts:imp:9999:1970-01-01:digest')).toEqual('5678')
  })

  it('locks segment impression digests', async () => {
    expect(await lock.lockDigest(redis, segment)).toEqual(segment)
    expect(await lock.lockDigest(redis, segment)).toEqual(segment)
    expect(await redis.get('dtcounts:imp:9999:1970-01-01:digest')).toEqual('5678')

    const changed = {...segment, digest: '5679'}
    expect(await lock.lockDigest(redis, changed)).toEqual({...changed, isDuplicate: true, cause: 'digestCache'})
    expect(await redis.get('dtcounts:imp:9999:1970-01-01:digest')).toEqual('5678')
  })

})
