const AWS = require('aws-sdk')
const s3 = new AWS.S3()
const { ArrangementInvalidError, MissingEnvError } = require('./errors')

/**
 * Actual s3 call
 */
exports._getObject = function(opts) {
  return s3.getObject(opts).promise()
}

/**
 * Get a string object from S3
 */
exports.getObject = async function(key) {
  if (!process.env.S3_BUCKET) {
    throw new MissingEnvError('You must provide an S3_BUCKET')
  }
  if (!process.env.S3_PREFIX) {
    throw new MissingEnvError('You must provide an S3_PREFIX')
  }
  const Bucket = process.env.S3_BUCKET
  const Key = process.env.S3_PREFIX + `/${key}`
  try {
    const resp = await exports._getObject({Bucket, Key})
    return resp.Body.toString()
  } catch (err) {
    if (err.statusCode == 404) {
      return null
    } else {
      throw err
    }
  }
}

/**
 * Get arrangement json from S3
 */
exports.getArrangement = async function(digest) {
  const str = await exports.getObject(`_arrangements/${digest}.json`)
  if (str) {
    try {
      return JSON.parse(str)
    } catch (err) {
      throw new ArrangementInvalidError(`Invalid json ${digest}`)
    }
  } else {
    return null
  }
}
