const zlib = require('zlib')
const { promisify } = require('util')
const { BadEventError } = require('./errors')

/**
 * decode kinesis records
 */
exports.decodeEvent = async function(event) {
  if (event && event.Records && event.Records.every(r => r.kinesis && r.kinesis.data)) {
    const decoders = event.Records.map(r => exports.decode(r.kinesis.data))
    const datas = await Promise.all(decoders)
    return [].concat.apply([], datas)
  } else {
    throw new BadEventError('Invalid kinesis event')
  }
}

/**
 * decode a base64'd gzip'd cloudwatch log event
 */
exports.decode = async function(str) {
  if (typeof(str) !== 'string') {
    throw new BadEventError('Invalid non-string data')
  }
  try {
    const buffer = Buffer.from(str, 'base64')
    const json = await promisify(zlib.gunzip)(buffer)
    const data = JSON.parse(json)
    const messages = (data.logEvents || []).map(e => {
      const parts = e.message.split('\t')
      const msg = JSON.parse(parts[2])
      return {...msg, time: e.timestamp}
    })
    return messages
  } catch (err) {
    throw new BadEventError('Invalid kinesis data', err)
  }
}
