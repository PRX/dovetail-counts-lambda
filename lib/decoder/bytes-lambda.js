const zlib = require('zlib')
const { promisify } = require('util')
const gunzip = promisify(zlib.gunzip)
const { BadEventError } = require('../errors')

/**
 * check for gzipped cloudwatch log events (subscription filter)
 *
 * https://www.npmjs.com/package/is-gzip
 */
exports.detect = function (buffer) {
  if (!buffer || buffer.length < 3) {
    return false
  }
  return buffer[0] === 0x1f && buffer[1] === 0x8b && buffer[2] === 0x08
}

/**
 * gunzip and json parse log events
 */
exports.decode = async function (buffer) {
  try {
    const json = await gunzip(buffer)
    const data = JSON.parse(json)
    return (data.logEvents || []).map(e => {
      const parts = e.message.split('\t')

      // formatted [time, guid, json] or [time, guid, level, json]
      if (parts.length === 3) {
        return { ...JSON.parse(parts[2]), time: e.timestamp }
      } else if (parts.length === 4) {
        return { ...JSON.parse(parts[3]), time: e.timestamp }
      } else {
        throw new Error(`Unrecognized message: ${e.message}`)
      }
    })
  } catch (err) {
    throw new BadEventError('Invalid kinesis data', err)
  }
}
