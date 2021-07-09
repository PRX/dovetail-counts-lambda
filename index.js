const log = require('lambda-log')
log.options.dynamicMeta = msg => (msg && msg._error ? { errorName: msg._error.name } : {})
const ByteRange = require('./lib/byte-range')
const decoder = require('./lib/decoder')
const kinesis = require('./lib/kinesis')
const RedisBackup = require('./lib/redis-backup')
const Arrangement = require('./lib/arrangement')
const { MissingEnvError, KinesisPutError } = require('./lib/errors')

const DEFAULT_SECONDS_THRESHOLD = 60
const DEFAULT_PERCENT_THRESHOLD = 0.99

/**
 * Process kinesis'd cloudwatch logged bytes-download. Send to BigQuery
 */
exports.handler = async event => {
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

    // kinesis downloads/impressions to be logged in batch
    const kinesisRecords = []

    // concurrently process each listener-episode+digest+day
    await Promise.all(
      decoded.map(async bytesData => {
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
          kinesisRecords.push({
            ...bytesData,
            bytes: total,
            seconds: totalSeconds,
            percent: totalPercent,
          })
        }

        // check which segments have been FULLY downloaded
        arr.segments.forEach(([firstByte, lastByte], idx) => {
          if (arr.isLoggable(idx) && range.complete(firstByte, lastByte)) {
            kinesisRecords.push({ ...bytesData, segment: idx })
          }
        })
      }),
    )

    // put kinesis records in batch
    const results = await kinesis.putWithLock(redis, kinesisRecords)

    // log results, throw a retryable error for any failures
    const info = { ...results, keys: decoded.length }
    log.info(`Sent ${results.overall} overall / ${results.segments} segments`, results)
    if (results.failed > 0) {
      throw new KinesisPutError(`Failed to put ${results.failed} kinesis records`, results.failed)
    }

    // return counts, for easy testing
    redis.disconnect().catch(err => null)
    return results
  } catch (err) {
    if (redis) {
      redis.disconnect().catch(err => null)
    }
    if (err.retryable) {
      if (err.count) {
        log.warn(err, { count: err.count })
      } else {
        log.warn(err)
      }
      throw err
    } else {
      log.error(err)
      return null
    }
  }
}
