const log = require('lambda-log')
const ByteRange = require('./lib/byte-range')
const decoder = require('./lib/kinesis-decoder')
const kinesis = require('./lib/kinesis')
const RedisBackup = require('./lib/redis-backup')
const Arrangement = require('./lib/arrangement')
const { MissingEnvError } = require('./lib/errors')

const DEFAULT_SECONDS_THRESHOLD = 60
const DEFAULT_PERCENT_THRESHOLD = 0.99

/**
 * Process kinesis'd cloudwatch logged bytes-download. Send to BigQuery
 */
exports.handler = async (event) => {
  let redis
  try {
    const decoded = await decoder.decodeEvent(event)

    // missing redis url is retryable ... to keep kinesis from moving on
    if (!process.env.REDIS_URL) {
      throw new MissingEnvError('You must set a REDIS_URL')
    }
    redis = new RedisBackup(process.env.REDIS_URL, process.env.REDIS_BACKUP_URL)

    // thresholds (these apply only to the file as a whole)
    const minSeconds = parseInt(process.env.SECONDS_THRESHOLD) || DEFAULT_SECONDS_THRESHOLD
    const minPercent = parseFloat(process.env.PERCENT_THRESHOLD) || DEFAULT_PERCENT_THRESHOLD

    // kinesis impressions to be logged in batch
    const kinesisOverall = []
    const kinesisSegments = []

    // concurrently process each listener-episode+digest+day
    await Promise.all(decoded.map(async (bytesData) => {
      const range = await ByteRange.load(bytesData.id, redis, bytesData.bytes)

      // lookup arrangement
      let arr
      try {
        arr = await Arrangement.load(bytesData.digest, redis)
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
      if (totalSeconds >= minSeconds || totalPercent >= minPercent) {
        kinesisOverall.push({...bytesData, bytes: total, seconds: totalSeconds, percent: totalPercent})
      }

      // check which segments have been FULLY downloaded
      arr.segments.forEach(([firstByte, lastByte], idx) => {
        if (arr.isLoggable(idx) && range.complete(firstByte, lastByte)) {
          kinesisSegments.push({...bytesData, segment: idx})
        }
      })
    }))

    // put kinesis records in batch (TODO: combine these somehow, and still get the counts)
    const overall = await kinesis.putWithLock(redis, kinesisOverall)
    const segments = await kinesis.putWithLock(redis, kinesisSegments)

    // log results
    const skipped = (kinesisOverall.length - overall) + (kinesisSegments.length - segments)
    const info = {overall, segments, keys: decoded.length}
    log.info(`Sent ${overall} overall / ${segments} segments`, info)
    if (skipped > 0) {
      log.info(`Skipped ${skipped}`)
    }

    // return all processed, for easy testing
    redis.disconnect().catch(err => null)
    return overall + segments
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
