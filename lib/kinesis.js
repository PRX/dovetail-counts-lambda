const AWS = require('aws-sdk')
const crypto = require('crypto')
const log = require('lambda-log')
const { KinesisPutError } = require('./errors')
const kinesis = new AWS.Kinesis({region: 'us-east-1'})

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
 * Record bigquery impressions
 */
exports.putImpression = async function({time, listenerSession, digest, segment, bytes, seconds, percent}) {
  const stream = getStream('KINESIS_IMPRESSION_STREAM')
  if (stream) {
    let rec = {
      timestamp: time || new Date().getTime(),
      listenerSession: listenerSession,
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
    return await exports.putRecord(stream, rec, listenerSession)
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
