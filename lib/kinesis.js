const AWS = require('aws-sdk')
const crypto = require('crypto')
const log = require('lambda-log')
const lock = require('./kinesis-lock')
const { KinesisPutError, MissingEnvError } = require('./errors')
const kinesis = new AWS.Kinesis({region: 'us-east-1'})

// export putRecords call, for testing/mocking
exports._putRecords = opts => kinesis.putRecords(opts).promise()

/**
 * Formatted kinesis impression records
 */
exports.format = function({time, le, digest, segment, bytes, seconds, percent}) {
  let rec = {
    timestamp: time || new Date().getTime(),
    listenerEpisode: le,
    digest: digest,
  }
  if (segment === undefined) {
    rec.type = 'bytes'
    rec.bytes = bytes
    rec.seconds = round(seconds, 2)
    rec.percent = round(percent, 4)
  } else {
    rec.type = 'segmentbytes'
    rec.segment = segment
  }
  return rec
}

/**
 * Lock redis and put records to kinesis
 */
exports.putWithLock = async function(redis, datas = [], maxChunk = 200) {
  if (datas.length === 0) {
    return 0
  } else if (datas.length > maxChunk) {
    const chunks = chunkArray(datas, maxChunk)
    const nums = await Promise.all(chunks.map(c => exports.putWithLock(redis, c, maxChunk)))
    return nums.reduce((a, b) => a + b, 0)
  } else {
    const records = datas.map(data => exports.format(data))

    // lock the segments/overall so we only fire once
    const locked = await Promise.all(records.map(rec => lock.lock(redis, rec)))

    // TODO: this is a bit temporary, but for now lock a listener-episode to
    // the 1st digest they download for that day.  mark downloads of any
    // other digest as duplicates
    const digestLocked = await(Promise.all(locked.map(rec => lock.lockDigest(redis, rec))))

    // log to kinesis
    const nonNulls = digestLocked.filter(rec => rec)
    const result = await putRecords(nonNulls)

    // unlock anything that failed (NOTE: this does not remove the digestLock,
    // as we have no way of knowing which already existed before now)
    await Promise.all(result.failed.map(data => lock.unlock(redis, data)))

    // throw a retryable error if we failed any kinesis putRecords
    if (result.failed.length > 0) {
      throw new KinesisPutError(`Failed to put ${result.failed.length} kinesis records`)
    }

    // return the number of non-dup putRecords
    return result.succeeded.filter(rec => !rec.isDuplicate).length
  }
}

// split array into chunks of max-size
function chunkArray(array, maxSize) {
  let i = 0;
  const chunks = [];
  const n = array.length;
  while (i < n) {
    chunks.push(array.slice(i, i += maxSize));
  }
  return chunks;
}

// Round floats to something sane
function round(num, places) {
  const mult = Math.pow(10, places)
  return Math.round(num * mult) / mult
}

// batch put kinesis records, returning succeeding/failing records
async function putRecords(rawRecords) {
  if (rawRecords.length === 0) {
    return {succeeded: [], failed: []}
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
    return {Data, PartitionKey}
  })

  try {
    const succeeded = []
    const failed = []
    const result = await exports._putRecords({StreamName, Records})
    result.Records.forEach(({ErrorCode, ErrorMessage}, idx) => {
      if (ErrorCode) {
        const err = new KinesisPutError('Kinesis putRecords partial failure')
        log.warn(err, {ErrorCode, ErrorMessage, Record: Records[idx]})
        failed.push(rawRecords[idx])
      } else {
        succeeded.push(rawRecords[idx])
      }
    })
    return {succeeded, failed}
  } catch (err) {
    const wrapped = new KinesisPutError(`Kinesis putRecords failure`, err)
    log.warn(wrapped, {Records})
    return {succeeded: [], failed: rawRecords}
  }
}
