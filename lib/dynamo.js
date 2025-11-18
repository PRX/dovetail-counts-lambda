const log = require('lambda-log')
const { GetItemCommand, DynamoDBClient } = require("@aws-sdk/client-dynamodb")
const { AssumeRoleCommand, STSClient } = require("@aws-sdk/client-sts")
const { ArrangementInvalidError, DynamodbGetError, MissingEnvError } = require('./errors')
const { NodeHttpHandler } = require('@smithy/node-http-handler')

const region = (process.env.ARRANGEMENTS_DDB_REGION || 'us-east-1').split(',')[0]

// NOTE: must use STS in the region containing the lambda, not the DDB table
// TODO: shouldn't this happen automagically, with region set?
const myRegion = process.env.AWS_REGION || 'us-east-1'
const requestHandler = new NodeHttpHandler({ connectionTimeout: 1000, requestTimeout: 2000 })
const sts = new STSClient({ region: myRegion, requestHandler })

// optionally load a client using a role
exports.client = async () => {
  if (process.env.ARRANGEMENTS_DDB_ACCESS_ROLE) {
    const RoleArn = process.env.ARRANGEMENTS_DDB_ACCESS_ROLE.split(',')[0]
    const RoleSessionName = 'dovetail-counts-lambda-dynamodb'
    const command = new AssumeRoleCommand({ RoleArn, RoleSessionName });

    try {
      const data = await sts.send(command)
      const { AccessKeyId, SecretAccessKey, SessionToken } = data.Credentials
      return new DynamoDBClient({
        region,
        requestHandler,
        credentials: {
          accessKeyId: AccessKeyId,
          secretAccessKey: SecretAccessKey,
          sessionToken: SessionToken
        }
      })
    } catch (err) {
      log.error(err, { msg: 'sts error', role: RoleArn })
      return new DynamoDBClient(region, requestHandler)
    }
  } else {
    return new DynamoDBClient(region, requestHandler)
  }
}

/**
 * Actual getItem call
 */
exports._getItem = async (params, client = null) => {
  client = client || (await exports.client())
  const command = new GetItemCommand(params)
  return client.send(command)
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

  if (result.Item?.data?.S) {
    try {
      return JSON.parse(result.Item.data.S)
    } catch (_err) {
      throw new ArrangementInvalidError(`Invalid json ${digest}: ${result.Item.data.S}`)
    }
  } else {
    return null
  }
}
