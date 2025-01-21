const log = require('lambda-log')
log.options.dynamicMeta = msg => (msg && msg._error ? { errorName: msg._error.name } : {})
const ByteRange = require('./lib/byte-range')
const decoder = require('./lib/decoder')
const dynamo = require('./lib/dynamo')
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
    let decoded = await decoder.decodeEvent(event)

    // optionally start/stop at specific timestamps
    if (process.env.PROCESS_AFTER) {
      const count = decoded.length
      const after = parseInt(process.env.PROCESS_AFTER)
      decoded = decoded.filter(d => d.time > after)
      log.info(`PROCESS_AFTER[${after}] processing ${decoded.length} / ${count} records`)
    } else if (process.env.PROCESS_UNTIL) {
      const count = decoded.length
      const until = parseInt(process.env.PROCESS_UNTIL)
      decoded = decoded.filter(d => d.time <= until)
      log.info(`PROCESS_UNTIL[${until}] processing ${decoded.length} / ${count} records`)
    }

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

    // only assumerole once for dynamodb access
    const ddbClient = await dynamo.client()

    // concurrently lookup arrangement json
    const digests = decoded.map(d => d.digest).reduce((acc, d) => ({ ...acc, [d]: null }), {})
    const loadArrangements = Object.keys(digests).map(async digest => {
      try {
        digests[digest] = await Arrangement.load(digest, redis, ddbClient)
      } catch (err) {
        if (err.skippable) {
          log.warn(err)
        } else {
          throw err
        }
      }
    })
    await Promise.all(loadArrangements)

    // filter/warn skippable arrangements
    const readyBytes = decoded.filter(bytesData => {
      if (digests[bytesData.digest]) {
        return true
      } else {
        log.warn('Skipping byte', { byte: bytesData })
      }
    })

    // concurrently process each listener-episode+digest+day
    await Promise.all(
      readyBytes.map(async bytesData => {
        const range = await ByteRange.load(bytesData.id, redis, bytesData.bytes)
        const arr = digests[bytesData.digest]
        const ads = arr.percentAds

        // check if the file-as-a-whole has been downloaded
        const bytesDownloaded = arr.segments.map(s => range.intersect(s))
        const total = bytesDownloaded.reduce((a, b) => a + b, 0)
        const totalSeconds = arr.bytesToSeconds(total)
        const totalPercent = arr.bytesToPercent(total)
        const isDownload = totalSeconds >= minSeconds || totalPercent >= minPercent
        if (isDownload) {
          kinesisRecords.push({
            ...bytesData,
            bytes: total,
            seconds: totalSeconds,
            percent: totalPercent,
            percentAds: ads,
          })
        }

        // check which segments have been FULLY downloaded
        arr.segments.forEach(([firstByte, lastByte], idx) => {
          if (isDownload && arr.isLoggable(idx)) {
            const pos = arr.segmentPosition(idx)
            const rec = { ...bytesData, segment: idx, segmentPosition: pos, percentAds: ads }

            // handle empty/zero-byte segments - just make sure their "firstByte" was downloaded
            if (firstByte > lastByte) {
              if (range.complete(firstByte, firstByte)) {
                kinesisRecords.push({ ...rec, isDuplicate: true, cause: 'empty' })
              }
            } else if (range.complete(firstByte, lastByte)) {
              kinesisRecords.push(rec)
            }
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
