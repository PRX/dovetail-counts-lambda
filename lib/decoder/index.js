const log = require('lambda-log')
const { BadEventError } = require('../errors')
const bytesLambda = require('./bytes-lambda')
const realTime = require('./real-time')
const standard = require('./standard')

function base64decode(str) {
  try {
    return Buffer.from(str, 'base64')
  } catch (err) {
    throw new BadEventError(`Invalid non-base64 data: ${str}`)
  }
}

/**
 * decode kinesis events
 */
exports.decodeEvent = async function (event) {
  if (event && event.Records && event.Records.every(r => r.kinesis && r.kinesis.data)) {
    const decoders = event.Records.map(r => exports.decode(r.kinesis.data))
    const datas = await Promise.all(decoders)
    const all = [].concat.apply([], datas).filter(d => d)
    return exports.formatResults(all)
  } else {
    throw new BadEventError('Invalid kinesis event')
  }
}

/**
 * decode a base64'd kinesis record
 */
exports.decode = async function (str) {
  try {
    const buffer = base64decode(str)
    if (bytesLambda.detect(buffer)) {
      return bytesLambda.decode(buffer)
    } else if (realTime.detect(buffer)) {
      return realTime.decode(buffer)
    } else if (standard.detect(buffer)) {
      return standard.decode(buffer)
    } else {
      throw new BadEventError(`Unrecognized data: ${str}`)
    }
  } catch (err) {
    if (err instanceof BadEventError) {
      log.warn(err)
      return null
    } else {
      throw err
    }
  }
}

/**
 * Format results, grouping by listener-episode + digest + UTC-day
 */
exports.formatResults = function (bytes) {
  let grouped = {}
  bytes.forEach(byte => {
    if (!byte.le) {
      return log.warn('Byte is missing le!')
    }
    if (!byte.digest) {
      return log.warn('Byte is missing digest!')
    }
    if (!byte.time) {
      log.warn('Byte is missing time')
      byte.time = new Date().getTime()
    }
    byte.day = new Date(byte.time).toISOString().substr(0, 10)
    byte.id = `${byte.le}/${byte.day}/${byte.digest}`

    const key = byte.id
    if (grouped[key]) {
      grouped[key].bytes.push(`${byte.start}-${byte.end}`)
      grouped[key].time = Math.max(grouped[key].time, byte.time)
      if (grouped[key].total !== byte.total) {
        log.warn(`Mismatched total for ${key} -> ${grouped[key].total} / ${byte.total}`)
      }
    } else {
      const { start, end, ...rest } = byte
      grouped[key] = { ...rest, bytes: [`${start}-${end}`] }
    }
  })
  return Object.values(grouped)
}
