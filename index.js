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
  try {
    const decoded = await decoder.decodeEvent(event)
    const uuids = Object.keys(decoded)
    const redis = new Redis()

    // thresholds
    const minSeconds = process.env.SECONDS_THRESHOLD || DEFAULT_SECONDS_THRESHOLD
    const minPercent = process.env.PERCENT_THRESHOLD || DEFAULT_PERCENT_THRESHOLD

    // concurrently process each request-uuid
    const handlers = uuids.map(async (uuid) => {
      const range = await ByteRange.load(uuid, redis, decoded[uuid].bytes)

      // lookup arrangement
      const arr = await Arrangement.load(decoded[uuid].digest, redis)

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

      // TODO: log downloads
      return {
        segments: isDownloaded,
        segmentBytes: bytesDownloaded,
        overall: overall,
        overallBytes: total,
      }
    })

    // return mapped results (for easy testing)
    const results = await Promise.all(handlers)
    return results.reduce((map, r, i) => { map[uuids[i]] = r; return map }, {})
  } catch (err) {
    console.error(err.stack)
    throw err
  }
}
