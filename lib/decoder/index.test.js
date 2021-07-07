const log = require('lambda-log')
const decoder = require('./index')
const EVENT = require('./bytes-lambda-event.test.json')
const REALTIME_EVENT = require('./real-time-event.test.json')

describe('decoder', () => {
  afterEach(() => jest.restoreAllMocks())

  it('decodes a known bytes-lambda event', async () => {
    const results = await decoder.decodeEvent(EVENT)
    expect(results.length).toEqual(25)
    expect(results[0]).toEqual({
      id: `${results[0].le}/2018-09-26/${results[0].digest}`,
      le: 'b1b7946d-50ad-47b9-a9df-df759071aac9',
      digest: 'e44AyWZHufBjPRC92aOaaTRHmc4wecVTM6d3ey4DgLY',
      day: '2018-09-26',
      region: 'us-west-2',
      time: 1537990270526,
      total: 70122363,
      bytes: ['0-70122362'],
    })
    expect(results[3]).toEqual({
      id: `${results[3].le}/2018-09-26/${results[3].digest}`,
      le: '6b2a0f7d-a668-4957-94a1-9a8a2587b908',
      digest: 'OFGIxCNq2fLa6Yb6tL18SF-wLnzQ4vKpi_xgzUKC0Y0',
      day: '2018-09-26',
      region: 'us-west-2',
      time: 1537990284309,
      total: 120766431,
      bytes: ['0-1', '0-120766430'],
    })
  })

  it('decodes a known real-time cloudfront logs event', async () => {
    const results = await decoder.decodeEvent(REALTIME_EVENT)
    expect(results.length).toEqual(1)
    expect(results[0]).toEqual({
      id: `${results[0].le}/2021-07-07/${results[0].digest}`,
      le: '11JaYha52T4SffQfFzsnARvfaI-8uZCJYC_baglk_2o',
      digest: 'XCRhICzauP2RsciquxM2EKSsdwWV1lQrkg1sABrceZA',
      day: '2021-07-07',
      time: 1625684148230,
      bytes: ['1000-2000', '0-645676', '2000-3000'],
    })
  })

  it('non-fatally removes single bad events', async () => {
    jest.spyOn(log, 'warn').mockImplementation(() => null)

    let bad = JSON.parse(JSON.stringify(EVENT))
    bad.Records[1].kinesis.data = 'messitup' + bad.Records[1].kinesis.data

    const results = await decoder.decodeEvent(bad)
    expect(Object.keys(results).length).toEqual(16) // 1 Record (9 logs)

    expect(log.warn).toHaveBeenCalledTimes(1)
    expect(log.warn.mock.calls[0][0].toString()).toMatch('BadEventError: Unrecognized data')
  })

  it('throws errors for invalid events', async () => {
    try {
      await decoder.decodeEvent({ Records: [{ kinesis: { no: 'data' } }] })
      fail('should have gotten an error')
    } catch (err) {
      expect(err.name).toEqual('BadEventError')
      expect(err.message).toMatch(/invalid kinesis event/i)
    }
  })

  it('warns for missing byte data', () => {
    jest.spyOn(log, 'warn').mockImplementation(() => null)

    const results1 = decoder.formatResults([{ le: null, digest: 'd', time: 1537990270526 }])
    expect(results1.length).toEqual(0)
    expect(log.warn).toHaveBeenCalledTimes(1)
    expect(log.warn.mock.calls[0][0].toString()).toMatch(/byte is missing le/i)

    const results2 = decoder.formatResults([{ le: 'le', digest: null, time: 1537990270526 }])
    expect(results2.length).toEqual(0)
    expect(log.warn).toHaveBeenCalledTimes(2)
    expect(log.warn.mock.calls[1][0].toString()).toMatch(/byte is missing digest/i)

    const results3 = decoder.formatResults([{ le: 'le', digest: 'd', time: null }])
    expect(results3.length).toEqual(1)
    expect(log.warn).toHaveBeenCalledTimes(3)
    expect(log.warn.mock.calls[2][0].toString()).toMatch(/byte is missing time/i)

    const results4 = decoder.formatResults([{ le: 'le', digest: 'd', time: 1537990270526 }])
    expect(results4.length).toEqual(1)
    expect(log.warn).toHaveBeenCalledTimes(3)
  })

  it('groups bytes by utc day', () => {
    const results = decoder.formatResults([
      { le: 'le', digest: 'd', time: 1537800000000, start: 0, end: 1 },
      { le: 'le', digest: 'd', time: 1537820000000, start: 1, end: 2 },
      { le: 'l2', digest: 'd', time: 1537840000000, start: 2, end: 3 },
      { le: 'le', digest: 'd', time: 1537810000000, start: 3, end: 4 },
      { le: 'le', digest: 'd', time: 1537850000000, start: 4, end: 5 },
    ])
    expect(results.length).toEqual(3)
    expect(results[0]).toMatchObject({ le: 'le', digest: 'd', day: '2018-09-24' })
    expect(results[0].time).toEqual(1537820000000)
    expect(results[0].bytes).toEqual(['0-1', '1-2', '3-4'])
    expect(results[1]).toMatchObject({ le: 'l2', digest: 'd', day: '2018-09-25' })
    expect(results[1].time).toEqual(1537840000000)
    expect(results[1].bytes).toEqual(['2-3'])
    expect(results[2]).toMatchObject({ le: 'le', digest: 'd', day: '2018-09-25' })
    expect(results[2].time).toEqual(1537850000000)
    expect(results[2].bytes).toEqual(['4-5'])
  })
})
