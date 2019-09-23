const log = require('lambda-log')
const kinesis = require('./kinesis')
const RedisBackup = require('./redis-backup')

describe('kinesis', () => {

  let lastData, allDatas
  beforeEach(() => {
    allDatas = []
    process.env.KINESIS_IMPRESSION_STREAM = 'stream-good'
    jest.spyOn(kinesis, '_putRecord').mockImplementation(async ({Data, StreamName, PartitionKey}) => {
      if (StreamName === 'stream-good') {
        lastData = Data
        allDatas.push(Data)
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
    expect(await kinesis.putImpression({le: '1234', digest: '5678'})).toEqual(null)
    process.env.KINESIS_IMPRESSION_STREAM = 'stream-good'
    expect(await kinesis.putImpression({le: '1234', digest: '5678'})).toEqual(true)
  })

  it('puts impressions data', async () => {
    expect(await kinesis.putImpression({le: '1234', digest: '5678', time: 9, bytes: 10, seconds: 12, percent: 0.4})).toEqual(true)
    expect(JSON.parse(lastData)).toEqual({
      type: 'bytes',
      timestamp: 9,
      listenerEpisode: '1234',
      digest: '5678',
      bytes: 10,
      seconds: 12,
      percent: 0.4,
    })
  })

  it('rounds download seconds and percents', async () => {
    expect(await kinesis.putImpression({seconds: 12.12345678, percent: 0.987654321})).toEqual(true)
    expect(JSON.parse(lastData).seconds).toEqual(12.12)
    expect(JSON.parse(lastData).percent).toEqual(0.9877)
  })

  it('puts segment impressions data', async () => {
    expect(await kinesis.putImpression({le: '1234', digest: '5678', segment: 0, time: 1})).toEqual(true)
    expect(JSON.parse(lastData)).toMatchObject({
      type: 'segmentbytes',
      listenerEpisode: '1234',
      digest: '5678',
      segment: 0,
      timestamp: 1,
    })
  })

  describe('with a redis connection', () => {

    let redis
    beforeEach(async () => {
      redis = new RedisBackup(process.env.REDIS_URL)
    })
    afterEach(async () => {
      await redis.nuke('dtcounts:imp:1234:*')
      await redis.nuke('dtcounts:imp:1235:*')
      await redis.disconnect()
    })

    it('locks the overall download', async () => {
      const data = {le: '1234', digest: '5678', time: 9, bytes: 10, seconds: 12, percent: 0.4}
      expect(await kinesis.putImpressionLock(redis, data)).toEqual(true)
      expect(await kinesis.putImpressionLock(redis, data)).toEqual(false)
      expect(await kinesis.putImpressionLock(redis, {...data, le: '1235'})).toEqual(true)

      expect(allDatas.length).toEqual(2)
      expect(JSON.parse(allDatas[0])).toMatchObject({listenerEpisode: '1234', digest: '5678', bytes: 10})
      expect(JSON.parse(allDatas[1])).toMatchObject({listenerEpisode: '1235', digest: '5678', bytes: 10})
    })

    it('locks segment impressions', async () => {
      const data = {le: '1234', digest: '5678', time: 9, bytes: 10, seconds: 12, percent: 0.4}
      expect(await kinesis.putImpressionLock(redis, {...data, segment: 0})).toEqual(true)
      expect(await kinesis.putImpressionLock(redis, {...data, segment: 0})).toEqual(false)
      expect(await kinesis.putImpressionLock(redis, {...data, segment: 3})).toEqual(true)
      expect(await kinesis.putImpressionLock(redis, {...data, segment: 2})).toEqual(true)
      expect(await kinesis.putImpressionLock(redis, {...data, segment: 3})).toEqual(false)

      expect(allDatas.length).toEqual(3)
      expect(JSON.parse(allDatas[0])).toMatchObject({listenerEpisode: '1234', digest: '5678', segment: 0})
      expect(JSON.parse(allDatas[1])).toMatchObject({listenerEpisode: '1234', digest: '5678', segment: 3})
      expect(JSON.parse(allDatas[2])).toMatchObject({listenerEpisode: '1234', digest: '5678', segment: 2})
    })

    it('locks the first digest to be downloaded each day', async () => {
      const data = {le: '1234', digest: '5678', time: 9, bytes: 10, seconds: 12, percent: 0.4}
      expect(await kinesis.putImpressionLock(redis, {...data, segment: 0})).toEqual(true)
      expect(await kinesis.putImpressionLock(redis, {...data, digest: '5679'})).toEqual(false)
      expect(await kinesis.putImpressionLock(redis, {...data, digest: '5679', segment: 3})).toEqual(false)
      expect(await kinesis.putImpressionLock(redis, {...data, digest: '5679', time: 86400001})).toEqual(true)
      expect(await kinesis.putImpressionLock(redis, {...data})).toEqual(true)

      expect(allDatas.length).toEqual(5)
      expect(JSON.parse(allDatas[0])).toMatchObject({digest: '5678', segment: 0})
      expect(JSON.parse(allDatas[0]).isDuplicate).toBeUndefined()
      expect(JSON.parse(allDatas[1])).toMatchObject({digest: '5679', isDuplicate: true, cause: 'digestCache'})
      expect(JSON.parse(allDatas[2])).toMatchObject({digest: '5679', isDuplicate: true, cause: 'digestCache', segment: 3})
      expect(JSON.parse(allDatas[3])).toMatchObject({digest: '5679', timestamp: 86400001})
      expect(JSON.parse(allDatas[3]).isDuplicate).toBeUndefined()
      expect(JSON.parse(allDatas[4])).toMatchObject({digest: '5678', timestamp: 9})
    })

  })

})
