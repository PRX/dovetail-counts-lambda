const s3 = require('./s3')

describe('s3', () => {

  const GOOD_DIGEST = 'dovetail-counts-lambda-good'
  const BAD_JSON_DIGEST = 'dovetail-counts-lambda-bad'

  let oldBucket, oldPrefix
  beforeEach(() => {
    oldBucket = process.env.S3_BUCKET
    oldPrefix = process.env.S3_PREFIX
    process.env.S3_BUCKET = 'prx-dovetail'
    process.env.S3_PREFIX = 'stitch-testing'
  })
  afterEach(() => {
    process.env.S3_BUCKET = oldBucket
    process.env.S3_PREFIX = oldPrefix
  })

  it('loads a json string from s3', async () => {
    const str = await s3.getObject(`_arrangements/${GOOD_DIGEST}.json`)
    expect(str).toContain('{"version":2,"data":{')
  })

  it('returns null for non-existent objects', async () => {
    expect(await s3.getObject('does/not/exist.json')).toEqual(null)
  })

  it('decodes arrangements', async () => {
    const json = await s3.getArrangement(GOOD_DIGEST)
    expect(json.version).toEqual(2)
    expect(Object.keys(json.data)).toEqual(['f', 't'])
  })

  it('errors on arrangement json decode', async () => {
    try {
      await s3.getArrangement(BAD_JSON_DIGEST)
      expect('').toEqual('should have gotten an error')
    } catch (err) {
      expect(err.message).toMatch(/invalid json/)
    }
  })

  it('returns null for non-existent arrangements', async () => {
    expect(await s3.getArrangement('does-not-exist')).toEqual(null)
  })

})
