const zlib = require('zlib')
const decoder = require('./kinesis-decoder')

describe('kinesis-decoder', () => {

  const EVENT = require('./kinesis-decoder.test.json')
  const DATA = EVENT.Records[0].kinesis.data

  it('decodes an event', async () => {
    const results = await decoder.decodeEvent(EVENT)
    expect(results.length).toEqual(28)
    expect(results[0]).toMatchObject({
      uuid: 'b1b7946d-50ad-47b9-a9df-df759071aac9',
      digest: 'e44AyWZHufBjPRC92aOaaTRHmc4wecVTM6d3ey4DgLY',
      region: 'us-west-2',
      start: 0,
      end: 70122362,
      total: 70122363,
      time: 1537990270526
    })
    expect(results[19]).toMatchObject({
      uuid: '23bad054-b988-475f-b345-4a5f41dcf61e',
      digest: 'MBZD8EhnrQt7FNSqlMUFaghdq8RPrDZUWRLvKNA0o14',
      region: 'us-west-2',
      start: 65536,
      end: 131071,
      total: 6926596,
      time: 1537990270347
    })
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
