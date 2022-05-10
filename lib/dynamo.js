const log = require('lambda-log')
const AWS = require('aws-sdk')
const { ArrangementInvalidError, DynamodbGetError, MissingEnvError } = require('./errors')

const region = (process.env.ARRANGEMENTS_DDB_REGION || 'us-east-1').split(',')[0]
const httpOptions = { connectTimeout: 1000, timeout: 2000 }
const maxRetries = 3
const clientOptions = { region, httpOptions, maxRetries }

// NOTE: must use STS in the region containing the lambda, not the DDB table
// TODO: shouldn't this happen automagically, with clientOptions.region set?
const myRegion = process.env.AWS_REGION || 'us-east-1'
const endpoint = new AWS.Endpoint(`sts.${myRegion}.amazonaws.com`)
const sts = new AWS.STS({ endpoint, httpOptions, maxRetries })

// optionally load a client using a role
exports.client = async () => {
  if (process.env.ARRANGEMENTS_DDB_ACCESS_ROLE) {
    const RoleArn = process.env.ARRANGEMENTS_DDB_ACCESS_ROLE.split(',')[0]
    const RoleSessionName = 'dovetail-counts-lambda-dynamodb'
    try {
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

  const TableName = process.env.ARRANGEMENTS_DDB_TABLE.split(',')[0]
  const params = { Key: { digest: { S: digest } }, TableName }

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
