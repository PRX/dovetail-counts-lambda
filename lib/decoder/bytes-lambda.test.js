const zlib = require('zlib')
const bytesLambda = require('./bytes-lambda')
const { BadEventError } = require('../errors')

describe('bytes-lambda', () => {
  afterEach(() => jest.restoreAllMocks())

  describe('#detect', () => {
    it('detects gzipped data', async () => {
      expect(bytesLambda.detect(zlib.gzipSync(''))).toEqual(true)
      expect(bytesLambda.detect(zlib.gzipSync('anything'))).toEqual(true)
      expect(bytesLambda.detect(zlib.gzipSync('{"some":"json"}'))).toEqual(true)
    })

    it('rejects other data', async () => {
      expect(bytesLambda.detect('')).toEqual(false)
      expect(bytesLambda.detect('anything')).toEqual(false)
      expect(bytesLambda.detect(Buffer.from('blah'))).toEqual(false)
    })
  })

  describe('#decode', () => {
    it('decodes cloudwatch log subscriptions', async () => {
      const byte = { le: '123', digest: '456', region: 'us-east-1', start: 0, end: 10, total: 100 }
      const rec = {
        logEvents: [
          { timestamp: 1234, message: `timestring\tguid\t${JSON.stringify(byte)}\n` },
          { timestamp: 5678, message: `timestring\tguid\tINFO\t${JSON.stringify(byte)}\n` },
        ],
      }
      const zipped = zlib.gzipSync(JSON.stringify(rec))

      const results = await bytesLambda.decode(zipped)
      expect(results.length).toEqual(2)
      expect(results[0]).toEqual({ ...byte, time: 1234 })
      expect(results[1]).toEqual({ ...byte, time: 5678 })
    })

    it('throws errors for bad gzip data', async () => {
      try {
        await bytesLambda.decode('not-zipped')
        fail('should have gotten an error')
      } catch (err) {
        expect(err).toBeInstanceOf(BadEventError)
        expect(err.message).toMatch(/invalid kinesis data/i)
        expect(err.message).toMatch(/unrecognized message/i)
      }
    })

    it('throws errors for bad json', async () => {
      try {
        await bytesLambda.decode(zlib.gzipSync('not-json'))
        fail('should have gotten an error')
      } catch (err) {
        expect(err).toBeInstanceOf(BadEventError)
        expect(err.message).toMatch(/invalid kinesis data/i)
        expect(err.message).toMatch(/unrecognized message/i)
      }
    })

    it('throws errors for bad logged lines', async () => {
      const byte = { le: '123', digest: '456', region: 'us-east-1', start: 0, end: 10, total: 100 }
      const rec = {
        logEvents: [
          { timestamp: 1234, message: `one\ttwo\tthree\tfour\t${JSON.stringify(byte)}\n` },
        ],
      }
      const zipped = zlib.gzipSync(JSON.stringify(rec))

      try {
        await bytesLambda.decode(zipped)
        fail('should have gotten an error')
      } catch (err) {
        expect(err).toBeInstanceOf(BadEventError)
        expect(err.message).toMatch(/invalid kinesis data/i)
        expect(err.message).toMatch(/unrecognized message/i)
      }
    })
  })
})
