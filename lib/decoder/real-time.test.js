const realTime = require('./real-time')

describe('real-time', () => {
  describe('#detect', () => {
    it('detects ascii with exactly 11 tabs', async () => {
      expect(realTime.detect('1\t2\t3\t4\t5\t6\t7\t8\t9\t10\t11')).toEqual(true)
      expect(realTime.detect('1\t2\t3\t4\t5\t6\t7\t8\t9\t10\t11\n')).toEqual(true)
    })

    it('rejects other data', async () => {
      expect(realTime.detect('')).toEqual(false)
      expect(realTime.detect('any\tthing')).toEqual(false)
      expect(realTime.detect(Buffer.from('blah'))).toEqual(false)
      expect(realTime.detect('1\t2\t3\t4\t5\t6\t7\t8\t9\t10')).toEqual(false)

      // 11 tabs, but non-ascii
      expect(realTime.detect('1\tקום\t3\t4\t5\t6\t7\t8\t9\t10\t11')).toEqual(false)
    })
  })

  describe('#decode', () => {
    let line
    beforeEach(() => {
      line = [
        '1234.567',
        '12.34.56.78',
        '206',
        'GET',
        '/123/my-guid/my-digest/file.mp3?a=b&le=my-le&c=d',
        'my-agent',
        'my-ref',
        '123.45.67.89',
        '1001',
        '1000',
        '2000\n',
      ]
    })

    it('decodes cloudfront real-time log lines', async () => {
      const result = await realTime.decode(line.join('\t'))
      expect(result).toEqual({
        time: 1234567,
        le: 'my-le',
        digest: 'my-digest',
        start: 1000,
        end: 2000,
      })
    })

    it('handles extra slashes in the uri stem', async () => {
      line[4] = '/123/my-guid/my-digest/file.mp3/?a=b&le=my-le&c=d'
      expect((await realTime.decode(line.join('\t'))).digest).toEqual('my-digest')

      line[4] = '/123/my-guid/my-digest/file.mp3?a=b&le=my/-le&c=d'
      expect((await realTime.decode(line.join('\t'))).digest).toEqual('my-digest')

      line[4] = '/123/my-guid/my-digest/file.mp3?a=b&le=my-le&c=d/'
      expect((await realTime.decode(line.join('\t'))).digest).toEqual('my-digest')
    })

    it('infers byte ranges', async () => {
      line[8] = '9999'
      line[9] = '-'
      line[10] = '-'
      const result = await realTime.decode(line.join('\t'))
      expect(result).toEqual({
        time: 1234567,
        le: 'my-le',
        digest: 'my-digest',
        start: 0,
        end: 9998,
      })
    })

    it('handles missing range end', async () => {
      line[8] = '11'
      line[9] = '10'
      line[10] = '-'
      const result = await realTime.decode(line.join('\t'))
      expect(result).toEqual({
        time: 1234567,
        le: 'my-le',
        digest: 'my-digest',
        start: 10,
        end: 20,
      })
    })

    it('returns null for non-GET requests', async () => {
      line[3] = 'POST'
      expect(await realTime.decode(line.join('\t'))).toBeNull()
    })

    it('requires a 2XX status', async () => {
      line[2] = '199'
      expect(await realTime.decode(line.join('\t'))).toBeNull()
      line[2] = '300'
      expect(await realTime.decode(line.join('\t'))).toBeNull()
      line[2] = '404'
      expect(await realTime.decode(line.join('\t'))).toBeNull()
      line[2] = '200'
      expect(await realTime.decode(line.join('\t'))).not.toBeNull()
    })

    it('requires a time', async () => {
      line[0] = '-'
      expect(await realTime.decode(line.join('\t'))).toBeNull()
    })

    it('requires a listener episode', async () => {
      line[4] = '/123/my-guid/my-digest/file.mp3'
      expect(await realTime.decode(line.join('\t'))).toBeNull()
      line[4] = '/123/my-guid/my-digest/file.mp3?a=b&le=&c=d'
      expect(await realTime.decode(line.join('\t'))).toBeNull()
    })

    it('requires a digest', async () => {
      line[4] = '/file.mp3?le=my-le'
      expect(await realTime.decode(line.join('\t'))).toBeNull()
    })

    it('requires a length', async () => {
      line[8] = '0'
      expect(await realTime.decode(line.join('\t'))).toBeNull()
      line[8] = '-'
      expect(await realTime.decode(line.join('\t'))).toBeNull()
    })
  })
})
