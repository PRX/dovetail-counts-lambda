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
  if (process.env.KINESIS_ARRANGEMENT_STREAM) {
    return await exports.putRecord(process.env.KINESIS_ARRANGEMENT_STREAM, digest)
  } else {
    return null
  }
}

/**
 * Record bigquery impressions
 */
exports.putBigQuery = async function(data) {
  if (process.env.KINESIS_BIGQUERY_STREAM) {
    return await exports.putRecord(process.env.KINESIS_BIGQUERY_STREAM, data, data.uuid)
  } else {
    return null
  }
}
