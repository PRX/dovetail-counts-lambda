const dynamo = require('./dynamo')
const { ArrangementInvalidError, DynamodbGetError, MissingEnvError } = require('./errors')

describe('dynamo', () => {
  beforeEach(() => (process.env.ARRANGEMENTS_DDB_TABLE = 'test-table-name'))

  it('gets arrangement json', async () => {
    const data = { some: 'arrangement', stuff: 'here' }

    jest.spyOn(dynamo, '_getItem').mockImplementation(async (params, client) => {
      expect(params).toEqual({
        TableName: 'test-table-name',
        Key: { digest: { S: 'some-digest' } },
      })
      expect(client).toBeNull()
      return { Item: { data: { S: JSON.stringify(data) } } }
    })

    expect(await dynamo.getArrangement('some-digest')).toEqual(data)
  })

  it('returns null for not found arrangements', async () => {
    jest.spyOn(dynamo, '_getItem').mockImplementation(async () => {
      return {}
    })

    expect(await dynamo.getArrangement('some-digest')).toEqual(null)
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
