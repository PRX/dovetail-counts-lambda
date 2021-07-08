const standard = require('./standard')

describe('standard', () => {
  describe('#detect', () => {
    it('detects ascii with exactly 33 fields', async () => {
      const arr = new Array(33).fill().map((_, i) => i)
      expect(standard.detect(arr.join(`\t`))).toEqual(true)
      expect(standard.detect(`${arr.join(`\t`)}\n`)).toEqual(true)
    })

    it('rejects other data', async () => {
      expect(standard.detect('')).toEqual(false)
      expect(standard.detect('any\tthing')).toEqual(false)
      expect(standard.detect(Buffer.from('blah'))).toEqual(false)

      const arr = new Array(34).fill().map((_, i) => i)
      expect(standard.detect(arr.join('\t'))).toEqual(false)

      // 33 tabs, but non-ascii
      arr.pop()
      arr[7] = 'tקום'
      expect(standard.detect(arr.join('\t'))).toEqual(false)
    })
  })

  describe('#decode', () => {
    let line
    beforeEach(() => {
      line = [
        '2021-07-07',
        '22:33:44',
        'my-edge-loc',
        '111111',
        '12.34.56.78',
        'GET',
        'my-host',
        '/123/my-guid/my-digest/file.mp3',
        '206',
        'my-ref',
        'my-agent',
        'a=b&le=my-le&c=d',
        '-',
        'Hit',
        '1234',
        'dovetail3-cdn.prxu.org',
        'https',
        '123',
        '456',
        '123.45.67.89',
        'TLSv1.3',
        'TLS_AES_128_GCM_SHA256',
        'Hit',
        'HTTP/2.0',
        '-',
        '-',
        '12345',
        '1.2',
        'Hit',
        'audio/mpeg',
        '1001',
        '1000',
        '2000\n',
      ]
    })

    it('decodes cloudfront standard log lines', async () => {
      const result = await standard.decode(line.join('\t'))
      expect(result).toEqual({
        time: 1625697224000,
        le: 'my-le',
        digest: 'my-digest',
        start: 1000,
        end: 2000,
      })
    })

    it('infers byte ranges', async () => {
      line[30] = '9999'
      line[31] = '-'
      line[32] = '-'
      const result = await standard.decode(line.join('\t'))
      expect(result).toEqual({
        time: 1625697224000,
        le: 'my-le',
        digest: 'my-digest',
        start: 0,
        end: 9998,
      })
    })

    it('handles missing range end', async () => {
      line[30] = '11'
      line[31] = '10'
      line[32] = '-'
      const result = await standard.decode(line.join('\t'))
      expect(result).toEqual({
        time: 1625697224000,
        le: 'my-le',
        digest: 'my-digest',
        start: 10,
        end: 20,
      })
    })

    it('returns null for non-GET requests', async () => {
      line[5] = 'POST'
      expect(await standard.decode(line.join('\t'))).toBeNull()
    })

    it('requires a 2XX status', async () => {
      line[8] = '199'
      expect(await standard.decode(line.join('\t'))).toBeNull()
      line[8] = '300'
      expect(await standard.decode(line.join('\t'))).toBeNull()
      line[8] = '404'
      expect(await standard.decode(line.join('\t'))).toBeNull()
      line[8] = '200'
      expect(await standard.decode(line.join('\t'))).not.toBeNull()
    })

    it('requires a time', async () => {
      line[0] = '-'
      expect(await standard.decode(line.join('\t'))).toBeNull()
    })

    it('requires a listener episode', async () => {
      line[11] = '-'
      expect(await standard.decode(line.join('\t'))).toBeNull()
      line[11] = 'a=b&le=&c=d'
      expect(await standard.decode(line.join('\t'))).toBeNull()
    })

    it('requires a digest', async () => {
      line[7] = '/file.mp3'
      expect(await standard.decode(line.join('\t'))).toBeNull()
    })

    it('requires a length', async () => {
      line[30] = '0'
      expect(await standard.decode(line.join('\t'))).toBeNull()
      line[30] = '-'
      expect(await standard.decode(line.join('\t'))).toBeNull()
    })
  })
})
