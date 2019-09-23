const ByteRange = require('./byte-range')
const RedisBackup = require('./redis-backup')

describe('byte-range', () => {

  const ID = 'byte-range/test/2018-09-25'
  const KEY = `dtcounts:bytes:${ID}`

  let redis
  beforeEach(() => {
    redis = new RedisBackup()
  })
  afterEach(async () => {
    await redis.del(KEY)
    await redis.disconnect()
  })

  it('loads new bytes', async () => {
    const range = await ByteRange.load(ID, redis)
    expect(range.bytes).toEqual([])
  })

  it('loads existing bytes', async () => {
    await redis.set(KEY, '10-20')
    const range = await ByteRange.load(ID, redis)
    expect(range.bytes).toEqual([[10, 20]])
  })

  it('appends existing bytes', async () => {
    await redis.set(KEY, '10-20,21-22')
    const range = await ByteRange.load(ID, redis, '15-30,99-100')
    expect(range.bytes).toEqual([[10, 30], [99, 100]])
  })

  it('decodes and combines byte ranges', () => {
    expect(ByteRange.decode()).toEqual([])
    expect(ByteRange.decode('')).toEqual([])
    expect(ByteRange.decode('10-20')).toEqual([[10, 20]])
    expect(ByteRange.decode('10-20,5-7,22-40,8-8,15-19')).toEqual([[5, 8], [10, 20], [22, 40]])
  })

  it('encodes byte ranges', () => {
    expect(new ByteRange().encode()).toEqual('')
    expect(new ByteRange('10-20,21-22,5-9').encode()).toEqual('5-22')
    expect(new ByteRange('22-23,10-20,4-12').encode()).toEqual('4-20,22-23')
  })

  it('calculates byte range intersections', () => {
    const range = new ByteRange('4-40,100-101,120-130')
    expect(range.intersect(0, 0)).toEqual(0)
    expect(range.intersect(0, 4)).toEqual(1)
    expect(range.intersect(0, 10)).toEqual(7)
    expect(range.intersect(0, 50)).toEqual(37)
    expect(range.intersect(8, 10)).toEqual(3)
    expect(range.intersect(30, 50)).toEqual(11)
    expect(range.intersect(100, 109)).toEqual(2)
    expect(range.intersect(4, 4)).toEqual(1)
    expect(range.intersect(3, 122)).toEqual(42)
  })

  it('checks if a segment is complete', () => {
    const range = new ByteRange('4-40,90-100,102-130')
    expect(range.complete(3, 40)).toEqual(false)
    expect(range.complete(4, 41)).toEqual(false)
    expect(range.complete(4, 40)).toEqual(true)
    expect(range.complete(41, 100)).toEqual(false)
    expect(range.complete(94, 110)).toEqual(false)
    expect(range.complete(104, 110)).toEqual(true)
  })

  it('totals bytes downloaded', () => {
    expect(new ByteRange('').total()).toEqual(0)
    expect(new ByteRange('0-0').total()).toEqual(1)
    expect(new ByteRange('0-10').total()).toEqual(11)
    expect(new ByteRange('4-40,10-20,99-100,100-101,120-130').total()).toEqual(51)
  })

})
