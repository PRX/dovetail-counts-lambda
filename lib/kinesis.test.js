const log = require('lambda-log')
const kinesis = require('./kinesis')

describe('kinesis', () => {

  let lastData
  beforeEach(() => {
    jest.spyOn(kinesis, '_putRecord').mockImplementation(async ({Data, StreamName, PartitionKey}) => {
      if (StreamName === 'stream-good') {
        lastData = Data
        return {mock: 'success'}
      } else if (StreamName === 'stream-bad') {
        throw new Error('Something something wrong')
      } else {
        throw new Error(`Unmocked kinesis request for ${StreamName}`)
      }
    })
  })

  afterEach(() => jest.restoreAllMocks())

  it('puts records', async () => {
    expect(await kinesis.putRecord('stream-good', 'mock-data')).toEqual(true)
  })

  it('warns kinesis errors', async () => {
    jest.spyOn(log, 'warn').mockImplementation(() => null)
    expect(await kinesis.putRecord('stream-bad', 'mock-data')).toEqual(false)
    expect(log.warn).toHaveBeenCalledTimes(1)
    expect(log.warn.mock.calls[0][0].toString()).toMatch('KinesisPutError: Kinesis putRecord failed for stream-bad')
    expect(log.warn.mock.calls[0][0].toString()).toMatch('Something something wrong')
  })

  it('puts missing arrangement digests', async () => {
    process.env.KINESIS_ARRANGEMENT_STREAM = ''
    expect(await kinesis.putMissingDigest('1234')).toEqual(null)
    process.env.KINESIS_ARRANGEMENT_STREAM = 'stream-good'
    expect(await kinesis.putMissingDigest('1234')).toEqual(true)
  })

  it('puts bigquery impressions', async () => {
    process.env.KINESIS_IMPRESSION_STREAM = ''
    expect(await kinesis.putImpression({ls: '1234', digest: '5678'})).toEqual(null)
    process.env.KINESIS_IMPRESSION_STREAM = 'stream-good'
    expect(await kinesis.putImpression({ls: '1234', digest: '5678'})).toEqual(true)
  })

  it('puts impressions data', async () => {
    process.env.KINESIS_IMPRESSION_STREAM = 'stream-good'
    expect(await kinesis.putImpression({ls: '1234', digest: '5678', time: 9, bytes: 10, seconds: 12, percent: 0.4})).toEqual(true)
    expect(JSON.parse(lastData)).toMatchObject({
      type: 'bytes',
      timestamp: 9,
      listenerSession: '1234',
      digest: '5678',
      bytes: 10,
      seconds: 12,
      percent: 0.4,
    })
  })

  it('rounds download seconds and percents', async () => {
    process.env.KINESIS_IMPRESSION_STREAM = 'stream-good'
    expect(await kinesis.putImpression({seconds: 12.12345678, percent: 0.987654321})).toEqual(true)
    expect(JSON.parse(lastData).seconds).toEqual(12.12)
    expect(JSON.parse(lastData).percent).toEqual(0.9877)
  })

  it('puts segment impressions data', async () => {
    process.env.KINESIS_IMPRESSION_STREAM = 'stream-good'
    expect(await kinesis.putImpression({ls: '1234', digest: '5678', segment: 0})).toEqual(true)
    expect(JSON.parse(lastData)).toMatchObject({
      type: 'segmentbytes',
      listenerSession: '1234',
      digest: '5678',
      segment: 0,
    })
    expect(JSON.parse(lastData).timestamp).toBeGreaterThan(new Date().getTime() - 2)
  })

})
