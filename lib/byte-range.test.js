const ByteRange = require('./byte-range')
const Redis = require('./redis')

describe('byte-range', () => {

  const UUID = 'test-uuid'
  const KEY = `dtcounts:bytes:${UUID}`

  let redis
  beforeEach(() => {
    redis = new Redis()
  })
  afterEach(async () => {
    await redis.del(KEY)
    await redis.disconnect()
  })

  it('loads new bytes', async () => {
    const range = await ByteRange.load(UUID, redis)
    expect(range.uuid).toEqual(UUID)
    expect(range.bytes).toEqual([])
  })

  it('loads existing bytes', async () => {
    await redis.set(KEY, '10-20')
    const range = await ByteRange.load(UUID, redis)
    expect(range.uuid).toEqual(UUID)
    expect(range.bytes).toEqual([[10, 20]])
  })

  it('appends existing bytes', async () => {
    await redis.set(KEY, '10-20,21-22')
    const range = await ByteRange.load(UUID, redis, '15-30,99-100')
    expect(range.uuid).toEqual(UUID)
    expect(range.bytes).toEqual([[10, 30], [99, 100]])
  })

  it('decodes and combines byte ranges', () => {
    expect(ByteRange.decode()).toEqual([])
    expect(ByteRange.decode('')).toEqual([])
    expect(ByteRange.decode('10-20')).toEqual([[10, 20]])
    expect(ByteRange.decode('10-20,5-7,22-40,8-8,15-19')).toEqual([[5, 8], [10, 20], [22, 40]])
  })

  it('encodes byte ranges', () => {
    expect(new ByteRange('uuid').encode()).toEqual('')
    expect(new ByteRange('uuid', '10-20,21-22,5-9').encode()).toEqual('5-22')
    expect(new ByteRange('uuid', '22-23,10-20,4-12').encode()).toEqual('4-20,22-23')
  })

  it('calculates byte range intersections', () => {
    const range = new ByteRange('uuid', '4-40,100-101,120-130')
    expect(range.intersect(0, 0)).toEqual(0)
    expect(range.intersect(100, 109)).toEqual(2)
    expect(range.intersect(4, 4)).toEqual(1)
    expect(range.intersect(3, 122)).toEqual(42)
  })

  it('totals bytes downloaded', () => {
    expect(new ByteRange('uuid', '').total()).toEqual(0)
    expect(new ByteRange('uuid', '0-0').total()).toEqual(1)
    expect(new ByteRange('uuid', '0-10').total()).toEqual(11)
    expect(new ByteRange('uuid', '4-40,10-20,99-100,100-101,120-130').total()).toEqual(51)
  })

})
