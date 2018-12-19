const log = require('lambda-log')
const ByteRange = require('./lib/byte-range')
const decoder = require('./lib/kinesis-decoder')
const kinesis = require('./lib/kinesis')
const Redis = require('./lib/redis')
const Arrangement = require('./lib/arrangement')

const DEFAULT_SECONDS_THRESHOLD = 60
const DEFAULT_PERCENT_THRESHOLD = 0.5

/**
 * Process kinesis'd cloudwatch logged bytes-download. Send to BigQuery
 */
exports.handler = async (event) => {
  let redis
  try {
    const decoded = await decoder.decodeEvent(event)
    const listenerSessionDigests = Object.keys(decoded)
    redis = new Redis()

    // thresholds (these apply only to the file as a whole)
    const minSeconds = parseInt(process.env.SECONDS_THRESHOLD) || DEFAULT_SECONDS_THRESHOLD
    const minPercent = parseFloat(process.env.PERCENT_THRESHOLD) || DEFAULT_PERCENT_THRESHOLD

    // concurrently process each listener-session+digest
    const handlers = listenerSessionDigests.map(async (lsd) => {
      const range = await ByteRange.load(lsd, redis, decoded[lsd].bytes)
      const listenerSession = range.listenerSession
      const digest = range.digest
      const time = decoded[lsd].time

      // lookup arrangement
      let arr
      try {
        arr = await Arrangement.load(digest, redis)
      } catch (err) {
        if (err.skippable) {
          log.warn(err)
          return false
        } else {
          throw err
        }
      }

      // check if the file-as-a-whole has been downloaded
      const bytesDownloaded = arr.segments.map(s => range.intersect(s))
      const total = bytesDownloaded.reduce((a, b) => a + b, 0)
      const totalSeconds = arr.bytesToSeconds(total)
      const totalPercent = arr.bytesToPercent(total)
      const hasSeconds = totalSeconds >= minSeconds
      const hasPercent = totalPercent >= minPercent
      const overall = hasSeconds ? 'seconds' : (hasPercent ? 'percent' : false)

      // start waiting for impressions
      let waiters = []
      if (overall) {
        waiters.push(kinesis.putImpression({time, listenerSession, digest, bytes: total, seconds: totalSeconds, percent: totalPercent}))
      } else {
        waiters.push(false)
      }

      // check which segments have been FULLY downloaded
      waiters = waiters.concat(arr.segments.map(async ([firstByte, lastByte], idx) => {
        if (range.complete(firstByte, lastByte)) {
          await kinesis.putImpression({time, listenerSession, digest, segment: idx})
          return true
        } else {
          return false
        }
      }))

      const [_, ...segmentImpressions] = await Promise.all(waiters)
      return {
        segments: segmentImpressions,
        overall: overall,
        overallBytes: total,
      }
    })

    // log results
    const handled = await Promise.all(handlers)
    const results = handled.filter(r => r)
    const countOverall = results.filter(r => r.overall).length
    const countSegments = results.map(r => r.segments.filter(s => s).length).reduce((a, b) => a + b, 0)
    const info = {overall: countOverall, segments: countSegments, keys: listenerSessionDigests.length}
    log.info(`Sent ${countOverall} overall / ${countSegments} segments`, info)
    if (handled.length > results.length) {
      log.info(`Skipped ${handled.length - results.length}`)
    }

    // return all processed, for easy testing
    redis.disconnect().catch(err => null)
    return results.reduce((map, r, i) => { map[listenerSessionDigests[i]] = r; return map }, {})
  } catch (err) {
    if (redis) {
      redis.disconnect().catch(err => null)
    }
    if (err.retryable) {
      log.error(err)
      throw err
    } else {
      log.error(err)
      return false
    }
  }
}
