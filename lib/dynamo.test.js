const { GetItemCommand, DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { AssumeRoleCommand, STSClient } = require("@aws-sdk/client-sts");
require("aws-sdk-client-mock-jest");
const { mockClient } = require("aws-sdk-client-mock");
const log = require('lambda-log')
const dynamo = require('./dynamo')
const { ArrangementInvalidError, DynamodbGetError, MissingEnvError } = require('./errors')

describe('dynamo', () => {
  beforeEach(() => { process.env.ARRANGEMENTS_DDB_TABLE = 'test-table-name' })

  it("returns a ddb client", async () => {
    const client = await dynamo.client();
    expect(client).toBeInstanceOf(DynamoDBClient);
  });

  it("assumes an sts role then returns a ddb client", async () => {
    process.env.ARRANGEMENTS_DDB_ACCESS_ROLE = "arn:aws:iam::1234:role/some-role";
    const stsMock = mockClient(STSClient);
    const Credentials = { AccessKeyId: "a", SecretAccessKey: "b", SessionToken: "c" };
    stsMock.on(AssumeRoleCommand).resolves({ Credentials });

    const client = await dynamo.client();
    expect(client).toBeInstanceOf(DynamoDBClient);
    const creds = await client.config.credentials();
    expect(creds.accessKeyId).toEqual("a");
    expect(creds.secretAccessKey).toEqual("b");
    expect(creds.sessionToken).toEqual("c");
  });

  it('gets arrangement json', async () => {
    const data = { some: 'arrangement', stuff: 'here' }
    const ddbMock = mockClient(DynamoDBClient)
    ddbMock.on(GetItemCommand).resolves({ Item: { data: { S: JSON.stringify(data) } } })

    const result = await dynamo.getArrangement('some-digest')

    expect(result).toEqual(data)
  
    expect(ddbMock).toHaveReceivedCommandWith(GetItemCommand, {
      Key: { digest: { S: "some-digest" } },
      TableName: process.env.ARRANGEMENTS_DDB_TABLE,
    });
  })

  it('returns null for not found arrangements', async () => {
    const ddbMock = mockClient(DynamoDBClient)
    ddbMock.on(GetItemCommand).resolves({})
    const result = await dynamo.getArrangement('some-digest')

    expect(result).toEqual(null)

    expect(ddbMock).toHaveReceivedCommandWith(GetItemCommand, {
      Key: { digest: { S: "some-digest" } },
      TableName: process.env.ARRANGEMENTS_DDB_TABLE,
    });
  })

  it('reuses dynamodb clients', async () => {
    jest.spyOn(dynamo, '_getItem').mockImplementation(async (_params, client) => {
      expect(client).toEqual({ some: 'client' })
      return {}
    })

    await dynamo.getArrangement('some-digest', { some: 'client' })
  })

  it('throws a skippable error for bad json', async () => {
    jest.spyOn(dynamo, '_getItem').mockImplementation(async () => {
      return { Item: { data: { S: '{not:json' } } }
    })

    try {
      await dynamo.getArrangement('some-digest')
      fail('should have gotten an error')
    } catch (err) {
      expect(err).toBeInstanceOf(ArrangementInvalidError)
      expect(err.message).toEqual('Invalid json some-digest: {not:json')
      expect(err.skippable).toEqual(true)
    }
  })

  it('throws a retryable error for missing env', async () => {
    process.env.ARRANGEMENTS_DDB_TABLE = ''

    try {
      await dynamo.getArrangement('some-digest')
      fail('should have gotten an error')
    } catch (err) {
      expect(err).toBeInstanceOf(MissingEnvError)
      expect(err.message).toEqual('You must provide a ARRANGEMENTS_DDB_TABLE')
      expect(err.retryable).toEqual(true)
    }
  })

  it('throws a retryable error for dynamodb problems', async () => {
    jest.spyOn(dynamo, '_getItem').mockImplementation(async () => {
      const bad = new Error('Blah blah throughput exceeded')
      bad.code = 'ProvisionedThroughputExceededException'
      bad.statusCode = 400
      throw bad
    })

    try {
      await dynamo.getArrangement('some-digest')
      fail('should have gotten an error')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamodbGetError)
      expect(err.message).toMatch(/blah blah throughput exceeded/i)
      expect(err.retryable).toEqual(true)
    }
  })
})
