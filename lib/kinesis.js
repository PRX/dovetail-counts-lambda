const { PutRecordsCommand, KinesisClient } = require("@aws-sdk/client-kinesis");
const log = require('lambda-log')
const lock = require('./kinesis-lock')
const { KinesisPutError, MissingEnvError } = require('./errors')

// note: these timeouts MUST be less than the execution timeout of the lambda,
// or else redis lock rollbacks will not work correctly
const region = process.env.AWS_REGION || 'us-east-1'
const httpOptions = { connectTimeout: 2000, timeout: 10000 }
const kinesis = new KinesisClient({ region, httpOptions, maxRetries: 3 })

exports._putRecords = async (params, client = null) => {
  const command = new PutRecordsCommand(params)
  const kinesisClient = client || kinesis
  return kinesisClient.send(command)
}

/**
 * Formatted kinesis impression records
 */
exports.format = ({
  time,
  le,
  digest,
  segment,
  bytes,
  seconds,
  percent,
  durations,
  types,
  isDuplicate,
  cause,
}) => {
  const rec = {
    timestamp: time || Date.now(),
    listenerEpisode: le,
    digest: digest,
  }
  if (isDuplicate) {
    rec.isDuplicate = true
    rec.cause = cause
  }
  if (segment === undefined) {
    rec.type = 'bytes'
    rec.bytes = bytes
    rec.seconds = round(seconds, 2)
    rec.percent = round(percent, 4)
    rec.durations = durations
    rec.types = types
  } else {
    rec.type = 'segmentbytes'
    rec.segment = segment
  }
  return rec
}

/**
 * Lock redis and put records to kinesis
 */
exports.putWithLock = async (redis, datas = [], maxChunk = 400) => {
  if (datas.length === 0) {
    return { overall: 0, segments: 0, overallDups: 0, segmentDups: 0, failed: 0 }
  } else if (datas.length > maxChunk) {
    const chunks = chunkArray(datas, maxChunk)
    const counts = await Promise.all(chunks.map(c => exports.putWithLock(redis, c, maxChunk)))
    return counts.reduce((acc, result) => {
      if (acc) {
        Object.keys(result).forEach(k => { acc[k] += result[k] })
        return acc
      } else {
        return result
      }
    })
  } else {
    const records = datas.map(data => exports.format(data))

    // lock the segments/overall so we only fire once
    const locked = await Promise.all(records.map(rec => lock.lock(redis, rec)))

    // TODO: this is a bit temporary, but for now lock a listener-episode to
    // the 1st digest they download for that day.  mark downloads of any
    // other digest as duplicates
    const digestLocked = await Promise.all(locked.map(rec => lock.lockDigest(redis, rec)))

    // log to kinesis
    const nonNulls = digestLocked.filter(rec => rec)
    const result = await putRecords(nonNulls)

    // unlock anything that failed (NOTE: this does not remove the digestLock,
    // as we have no way of knowing which already existed before now)
    await Promise.all(result.failed.map(data => lock.unlock(redis, data)))

    // return the counts of what we did
    return {
      overall: result.succeeded.filter(rec => rec.type === 'bytes' && !rec.isDuplicate).length,
      segments: result.succeeded.filter(rec => rec.type === 'segmentbytes' && !rec.isDuplicate)
        .length,
      overallDups: result.succeeded.filter(rec => rec.type === 'bytes' && rec.isDuplicate).length,
      segmentDups: result.succeeded.filter(rec => rec.type === 'segmentbytes' && rec.isDuplicate)
        .length,
      failed: result.failed.length,
    }
  }
}

// split array into chunks of max-size
function chunkArray(array, maxSize) {
  let i = 0
  const chunks = []
  const n = array.length
  while (i < n) {
    const nextIndex = i + maxSize
    chunks.push(array.slice(i, nextIndex))
    i = nextIndex
  }
  return chunks
}

// Round floats to something sane
function round(num, places) {
  const mult = Math.pow(10, places)
  return Math.round(num * mult) / mult
}

// batch put kinesis records, returning succeeding/failing records
async function putRecords(rawRecords) {
  if (rawRecords.length === 0) {
    return { succeeded: [], failed: [] }
  }

  let StreamName = process.env['KINESIS_IMPRESSION_STREAM']
  if (!StreamName) {
    throw new MissingEnvError('You must set KINESIS_IMPRESSION_STREAM')
  } else if (StreamName.indexOf('/') > -1) {
    StreamName = StreamName.split('/').pop()
  }

  const Records = rawRecords.map(rec => {
    const Data = JSON.stringify(rec)
    const PartitionKey = rec.listenerEpisode
    return { Data, PartitionKey }
  })

  try {
    const succeeded = []
    const failed = []
    const result = await exports._putRecords({ StreamName, Records })
    result.Records.forEach(({ ErrorCode, ErrorMessage }, idx) => {
      if (ErrorCode) {
        const err = new KinesisPutError('Kinesis putRecords partial failure')
        log.warn(err, { ErrorCode, ErrorMessage, Record: Records[idx] })
        failed.push(rawRecords[idx])
      } else {
        succeeded.push(rawRecords[idx])
      }
    })
    return { succeeded, failed }
  } catch (err) {
    const wrapped = new KinesisPutError(`Kinesis putRecords failure`, err)
    log.warn(wrapped, { Records })
    return { succeeded: [], failed: rawRecords }
  }
}
