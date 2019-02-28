const AWS = require('aws-sdk')
const crypto = require('crypto')
const log = require('lambda-log')
const { KinesisPutError } = require('./errors')
const kinesis = new AWS.Kinesis({region: 'us-east-1'})
const DEFAULT_TTL = 86400

/**
 * Actual kinesis call
 */
exports._putRecord = function(opts) {
  return kinesis.putRecord(opts).promise()
}

/**
 * Wrapper for kinesis putRecord
 */
exports.putRecord = async function(StreamName, Data, PartitionKey = null) {
  if (typeof Data !== 'string') {
    Data = JSON.stringify(Data)
  }
  if (!PartitionKey) {
    PartitionKey = crypto.createHash('md5').update(Data).digest('hex')
  }
  try {
    await exports._putRecord({Data, StreamName, PartitionKey})
    return true
  } catch (err) {
    const wrapped = new KinesisPutError(`Kinesis putRecord failed for ${StreamName}`, err)
    log.warn(wrapped)
    return false
  }
}

/**
 * Record missing digests
 */
exports.putMissingDigest = async function(digest) {
  const stream = getStream('KINESIS_ARRANGEMENT_STREAM')
  if (stream) {
    return await exports.putRecord(stream, digest)
  } else {
    return null
  }
}

/**
 * Record bigquery impressions with redis lock
 */
exports.putImpressionLock = async function(redis, {le, digest, time, segment, ...data}) {
  const day = (time ? new Date(time) : new Date).toISOString().substr(0, 10)
  const key = `dtcounts:imp:${le}:${day}:${digest}`
  const fld = segment === undefined ? 'all' : segment
  const ttl = process.env.REDIS_IMPRESSION_TTL || DEFAULT_TTL
  if (await redis.lock(key, fld, ttl)) {
    // TODO: this is a bit temporary, but for now lock a listener-episode to
    // the 1st digest they download for that day.  mark downloads of any
    // other digest as duplicates
    if (await redis.lockValue(`dtcounts:imp:${le}:${day}:digest`, digest, ttl)) {
      return await exports.putImpression({...data, le, digest, time, segment})
    } else {
      await exports.putImpression({...data, le, digest, time, segment, dup: true})
      return false
    }
  } else {
    return false
  }
}

/**
 * Record bigquery impressions
 */
exports.putImpression = async function({time, le, digest, segment, bytes, seconds, percent, dup}) {
  const stream = getStream('KINESIS_IMPRESSION_STREAM')
  if (stream) {
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
    if (dup) {
      rec.isDuplicate = true
      rec.cause = 'digestCache'
    }
    return await exports.putRecord(stream, rec, le)
  } else {
    return null
  }
}

/**
 * Lookup a stream name from an ENV variable
 */
function getStream(name) {
  if (process.env[name]) {
    const val = process.env[name]
    if (val.indexOf('/') > -1) {
      return val.split('/').pop()
    } else {
      return val
    }
  } else {
    return false
  }
}

/**
 * Round floats to something sane
 */
function round(num, places) {
  const mult = Math.pow(10, places)
  return Math.round(num * mult) / mult
}
