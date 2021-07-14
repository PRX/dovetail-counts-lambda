const log = require('lambda-log')
const AWS = require('aws-sdk')
const sts = new AWS.STS()
const { ArrangementInvalidError, DynamodbGetError, MissingEnvError } = require('./errors')

const region = process.env.ARRANGEMENTS_DDB_REGION || 'us-east-1'
const clientOptions = { region, maxRetries: 5, httpOptions: { timeout: 1000 } }

// optionally load a client using a role
exports.client = async () => {
  if (process.env.ARRANGEMENTS_DDB_ACCESS_ROLE) {
    try {
      const RoleArn = process.env.ARRANGEMENTS_DDB_ACCESS_ROLE
      const RoleSessionName = 'dovetail-counts-lambda-dynamodb'
      const data = await sts.assumeRole({ RoleArn, RoleSessionName }).promise()
      const { AccessKeyId, SecretAccessKey, SessionToken } = data.Credentials
      return new AWS.DynamoDB({
        ...clientOptions,
        accessKeyId: AccessKeyId,
        secretAccessKey: SecretAccessKey,
        sessionToken: SessionToken,
      })
    } catch (err) {
      log.error(err, { msg: 'sts error', role: RoleArn })
      return new AWS.DynamoDB(clientOptions)
    }
  } else {
    return new AWS.DynamoDB(clientOptions)
  }
}

/**
 * Actual getItem call
 */
exports._getItem = async (params, client = null) => {
  client = client || (await exports.client())
  return client.getItem(params).promise()
}

/**
 * Get an arrangement json
 */
exports.getArrangement = async (digest, client = null) => {
  if (!process.env.ARRANGEMENTS_DDB_TABLE) {
    throw new MissingEnvError('You must provide a ARRANGEMENTS_DDB_TABLE')
  }

  const params = { Key: { digest: { S: digest } }, TableName: process.env.ARRANGEMENTS_DDB_TABLE }

  let result = null
  try {
    result = await exports._getItem(params, client)
  } catch (err) {
    throw new DynamodbGetError(`Failed to get ${digest}`, err)
  }

  if (result.Item && result.Item.data && result.Item.data.S) {
    try {
      return JSON.parse(result.Item.data.S)
    } catch (err) {
      throw new ArrangementInvalidError(`Invalid json ${digest}: ${result.Item.data.S}`)
    }
  } else {
    return null
  }
}
