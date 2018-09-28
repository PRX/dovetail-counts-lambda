const zlib = require('zlib')
const log = require('lambda-log')
const decoder = require('./kinesis-decoder')

describe('kinesis-decoder', () => {

  const EVENT = require('./kinesis-decoder.test.json')
  const DATA = EVENT.Records[0].kinesis.data

  afterEach(() => jest.resetAllMocks())

  it('decodes an event', async () => {
    const results = await decoder.decodeEvent(EVENT)
    expect(Object.keys(results).length).toEqual(25)
    expect(results['b1b7946d-50ad-47b9-a9df-df759071aac9'].bytes.length).toEqual(1)
    expect(results['23bad054-b988-475f-b345-4a5f41dcf61e'].bytes.length).toEqual(1)
    expect(results['6b2a0f7d-a668-4957-94a1-9a8a2587b908'].bytes.length).toEqual(2)
  })

  it('simplifies event data', async () => {
    const results = await decoder.decodeEvent(EVENT)
    expect(results['b1b7946d-50ad-47b9-a9df-df759071aac9']).toMatchObject({
      digest: 'e44AyWZHufBjPRC92aOaaTRHmc4wecVTM6d3ey4DgLY',
      region: 'us-west-2',
      total: 70122363,
      bytes: [[0, 70122362]]
    })
    expect(results['6b2a0f7d-a668-4957-94a1-9a8a2587b908']).toMatchObject({
      digest: 'OFGIxCNq2fLa6Yb6tL18SF-wLnzQ4vKpi_xgzUKC0Y0',
      region: 'us-west-2',
      total: 120766431,
      bytes: [[0, 1], [0, 120766430]]
    })
  })

  it('non-fatally removes single bad events', async () => {
    jest.spyOn(log, 'warn').mockImplementation(() => null)

    let bad = JSON.parse(JSON.stringify(EVENT))
    bad.Records[1].kinesis.data = 'messitup' + bad.Records[1].kinesis.data

    const results = await decoder.decodeEvent(bad)
    expect(Object.keys(results).length).toEqual(16) // 1 Record (9 logs)

    expect(log.warn).toHaveBeenCalledTimes(1)
    expect(log.warn.mock.calls[0][0].toString()).toMatch('BadEventError: Invalid kinesis data')
  })

  it('throws errors for invalid events', async () => {
    try {
      await decoder.decodeEvent({Records: [{kinesis: {no: 'data'}}]})
      fail('should have gotten an error')
    } catch (err) {
      expect(err.name).toEqual('BadEventError')
      expect(err.message).toMatch(/invalid kinesis event/i)
    }
  })

  it('decodes a single data', async () => {
    const results = await decoder.decode(DATA)
    expect(results.length).toEqual(19)
    expect(results[0]).toMatchObject({
      uuid: 'b1b7946d-50ad-47b9-a9df-df759071aac9',
      digest: 'e44AyWZHufBjPRC92aOaaTRHmc4wecVTM6d3ey4DgLY',
      region: 'us-west-2',
      start: 0,
      end: 70122362,
      total: 70122363,
      time: 1537990270526
    })
  })

  it('throws errors for invalid gzip data', async () => {
    try {
      const bad = 'hello' + DATA
      await decoder.decode(bad)
      fail('should have gotten an error')
    } catch (err) {
      expect(err.name).toEqual('BadEventError')
      expect(err.message).toMatch(/invalid kinesis data/i)
    }
  })

})
