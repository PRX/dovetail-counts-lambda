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
 * Format results, grouping by listener-session + digest (lsd) and de-duping fields
 */
exports.formatResults = function(bytes) {
  let grouped = {}
  bytes.forEach(byte => {
    if (!byte.ls || !byte.digest) {
      log.warn(`Byte is missing 'ls' or 'digest'!`)
      return
    }

    const lsd = `${byte.ls}/${byte.digest}`
    if (grouped[lsd]) {
      grouped[lsd].bytes.push(`${byte.start}-${byte.end}`)
      grouped[lsd].time = Math.max(grouped[lsd].time, byte.time)
      if (grouped[lsd].total !== byte.total) {
        log.warn(`Mismatched total for ${lsd} -> ${grouped[lsd].total} / ${byte.total}`)
      }
    } else {
      const {start, end, ls, digest, ...rest} = byte
      grouped[lsd] = {...rest, bytes: [`${start}-${end}`]}
    }
  })
  return grouped
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
