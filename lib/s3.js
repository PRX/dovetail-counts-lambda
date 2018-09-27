const AWS = require('aws-sdk')
const s3 = new AWS.S3()
const { ArrangementInvalidError, MissingEnvError } = require('./errors')

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
    const resp = await s3.getObject({Bucket, Key}).promise()
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
