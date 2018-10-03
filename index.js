const log = require('lambda-log')
const ByteRange = require('./lib/byte-range')
const decoder = require('./lib/kinesis-decoder')
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
    const uuids = Object.keys(decoded)
    redis = new Redis()

    // thresholds
    const minSeconds = parseInt(process.env.SECONDS_THRESHOLD) || DEFAULT_SECONDS_THRESHOLD
    const minPercent = parseFloat(process.env.PERCENT_THRESHOLD) || DEFAULT_PERCENT_THRESHOLD

    // concurrently process each request-uuid
    const handlers = uuids.map(async (uuid) => {
      const range = await ByteRange.load(uuid, redis, decoded[uuid].bytes)

      // lookup arrangement
      let arr
      try {
        arr = await Arrangement.load(decoded[uuid].digest, redis)
      } catch (err) {
        if (err.skippable) {
          log.warn(err)
          return false
        } else {
          throw err
        }
      }

      // check which segments have been downloaded
      const bytesDownloaded = arr.segments.map(s => range.intersect(s))
      const isDownloaded = bytesDownloaded.map((bytes, idx) => {
        if (arr.bytesToSeconds(bytes) >= minSeconds) {
          return 'seconds'
        } else if (arr.bytesToPercent(bytes, idx) >= minPercent) {
          return 'percent'
        } else {
          return false
        }
      })

      // check if the file-as-a-whole has been downloaded
      const total = bytesDownloaded.reduce((a, b) => a + b, 0)
      const hasSeconds = arr.bytesToSeconds(total) >= minSeconds
      const hasPercent = arr.bytesToPercent(total) >= minPercent
      const overall = hasSeconds ? 'seconds' : (hasPercent ? 'percent' : false)

      // TODO: log downloads to bigquery kinesis
      return {
        segments: isDownloaded,
        segmentBytes: bytesDownloaded,
        overall: overall,
        overallBytes: total,
      }
    })

    // log results
    const handled = await Promise.all(handlers)
    const results = handled.filter(r => r)
    const countOverall = results.filter(r => r.overall).length
    const countSegments = results.map(r => r.segments.filter(s => s).length).reduce((a, b) => a + b, 0)
    const info = {overall: countOverall, segments: countSegments, uuids: uuids.length}
    log.info(`Sent ${countOverall} overall / ${countSegments} segments`, info)
    if (handled.length > results.length) {
      log.info(`Skipped ${handled.length - results.length}`)
    }

    // return all processed, for easy testing
    redis.disconnect()
    return results.reduce((map, r, i) => { map[uuids[i]] = r; return map }, {})
  } catch (err) {
    if (redis) {
      redis.disconnect()
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
