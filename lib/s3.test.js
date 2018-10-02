const s3 = require('./s3')

describe('s3', () => {

  const JSON = '{"version":2,"data":{"f":["http://fake.dovetail.host/test/files/meta.mp3","http://fake.dovetail.host/test/files/t01.mp3","http://fake.dovetail.host/test/files/t03.mp3","http://fake.dovetail.host/test/files/t05.mp3"],"t":"oaaa"}}'

  let bucket, prefix
  beforeEach(() => {
    bucket = process.env.S3_BUCKET
    prefix = process.env.S3_PREFIX
    jest.spyOn(s3, '_getObject').mockImplementation(async ({Key}) => {
      if (Key.endsWith('good.json')) {
        return {Body: JSON}
      } else if (Key.endsWith('bad.json')) {
        return {Body: 'badjson' + JSON}
      } else if (Key.endsWith('does-not-exist.json')) {
        const err = new Error('Thing not found')
        err.statusCode = 404
        throw err
      } else {
        throw new Error(`Unmocked S3 request: ${Key}`)
      }
    })
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
    const str = await s3.getObject(`_arrangements/good.json`)
    expect(str).toContain('{"version":2,"data":{')
  })

  it('returns null for non-existent objects', async () => {
    expect(await s3.getObject('does-not-exist.json')).toEqual(null)
  })

  it('decodes arrangements', async () => {
    const json = await s3.getArrangement('good')
    expect(json.version).toEqual(2)
    expect(Object.keys(json.data)).toEqual(['f', 't'])
  })

  it('errors on arrangement json decode', async () => {
    try {
      await s3.getArrangement('bad')
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
