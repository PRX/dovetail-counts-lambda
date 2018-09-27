const s3 = require('./s3')

describe('s3', () => {

  const GOOD_DIGEST = 'dovetail-counts-lambda-good'
  const BAD_JSON_DIGEST = 'dovetail-counts-lambda-bad'

  let bucket, prefix
  beforeEach(() => {
    bucket = process.env.S3_BUCKET
    prefix = process.env.S3_PREFIX
  })
  afterEach(() => {
    process.env.S3_BUCKET = bucket
    process.env.S3_PREFIX = prefix
  })

  it('requires a bucket', async () => {
    delete process.env.S3_BUCKET
    try {
      await s3.getObject('whatev')
      fail('should have gotten an error')
    } catch (err) {
      expect(err.name).toEqual('MissingEnvError')
      expect(err.message).toMatch(/S3_BUCKET/i)
    }
  })

  it('requires a prefix', async () => {
    delete process.env.S3_PREFIX
    try {
      await s3.getObject('whatev')
      fail('should have gotten an error')
    } catch (err) {
      expect(err.name).toEqual('MissingEnvError')
      expect(err.message).toMatch(/S3_PREFIX/i)
    }
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
      fail('should have gotten an error')
    } catch (err) {
      expect(err.name).toEqual('ArrangementInvalidError')
      expect(err.message).toMatch(/invalid json/i)
    }
  })

  it('returns null for non-existent arrangements', async () => {
    expect(await s3.getArrangement('does-not-exist')).toEqual(null)
  })

})
