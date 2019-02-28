const zlib = require('zlib')
const log = require('lambda-log')
const { promisify } = require('util')
const { BadEventError } = require('./errors')

/**
 * decode kinesis records
 */
exports.decodeEvent = async function(event) {
  if (event && event.Records && event.Records.every(r => r.kinesis && r.kinesis.data)) {
    const decoders = event.Records.map(r => {
      return exports.decode(r.kinesis.data).catch(err => {
        if (err instanceof BadEventError) {
          log.warn(err)
        } else {
          throw err
        }
      })
    })
    const datas = await Promise.all(decoders)
    const all = [].concat.apply([], datas).filter(d => d)
    return exports.formatResults(all)
  } else {
    throw new BadEventError('Invalid kinesis event')
  }
}

/**
 * Format results, grouping by listener-episode + digest + UTC-day
 */
exports.formatResults = function(bytes) {
  let grouped = {}
  bytes.forEach(byte => {
    if (!byte.le) {
      return log.warn('Byte is missing le!')
    }
    if (!byte.digest) {
      return log.warn('Byte is missing digest!')
    }
    if (!byte.time) {
      log.warn('Byte is missing time');
      byte.time = new Date().getTime();
    }
    byte.day = new Date(byte.time).toISOString().substr(0, 10)
    byte.id = `${byte.le}/${byte.digest}/${byte.day}`

    const key = byte.id
    if (grouped[key]) {
      grouped[key].bytes.push(`${byte.start}-${byte.end}`)
      grouped[key].time = Math.max(grouped[key].time, byte.time)
      if (grouped[key].total !== byte.total) {
        log.warn(`Mismatched total for ${key} -> ${grouped[key].total} / ${byte.total}`)
      }
    } else {
      const {start, end, ...rest} = byte
      grouped[key] = {...rest, bytes: [`${start}-${end}`]}
    }
  })
  return Object.values(grouped)
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

/**
 * TODO: what the what?
 */
exports._decode = async function(data) {
  // A: works fine
  return await [data].map(d => exports.decode(d))[0]

  // B: Jest fails with open zlib handle
  // return await exports.decode(data)

  // C: Also fails with open zlib handle
  // return await [exports.decode(data)][0]
}
