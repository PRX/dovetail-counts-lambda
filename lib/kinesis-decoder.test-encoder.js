const zlib = require('zlib')

/**
 * Encode logged events to the "cloudwatch -> kinesis" subscription event format
 */
exports.encodeLambdaEvent = (logEvents) => {
  const subscription = exports.encodeSubscription(logEvents)
  const json = JSON.stringify(subscription)
  const encoded = zlib.gzipSync(json).toString('base64')
  return {
    Records: [
      {
        kinesis: {
          kinesisSchemaVersion: '1.0',
          partitionKey: 'some-string',
          sequenceNumber: "1234567890",
          data: encoded,
          approximateArrivalTimestamp: 1537000000.123
        },
        eventSource: 'aws:kinesis',
        eventVersion: '1.0',
        eventID: 'shardId-000000000000:987654321',
        eventName: 'aws:kinesis:record',
        invokeIdentityArn: 'arn:aws:iam::1234:role/my_role',
        awsRegion: 'us-east-1',
        eventSourceARN: 'arn:aws:kinesis:us-east-1:1234:stream/my_stream'
      }
    ]
  }
}

/**
 * Encode log events to subscription
 */
exports.encodeSubscription = (logEvents) => {
  return {
    messageType: 'DATA_MESSAGE',
    owner: '1234',
    logGroup: '/aws/lambda/us-east-1.my-function-name',
    logStream: 'log-stream-name',
    subscriptionFilters: ['filter-name'],
    logEvents: logEvents.map(e => exports.encodeLogEvent(e)),
  }
}

/**
 * Encode a log event ... should be [timestamp, message]
 */
exports.encodeLogEvent = ([timestamp, message]) => {
  if (!timestamp || !message) {
    throw new Error('Pass in log events with format [timestamp, message]')
  }
  const dateStr = new Date(timestamp).toISOString()
  const random = Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15)
  return {
    id: 'this-doesnot-matter',
    timestamp: timestamp,
    message: `${dateStr}\t${random}\t${message}\n`,
  }
}
