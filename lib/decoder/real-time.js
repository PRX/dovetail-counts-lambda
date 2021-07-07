const url = require('url')

// https://github.com/PRX/Infrastructure/blob/master/cdn/dovetail3-cdn.yml#L289
const FIELDS = [
  'timestamp',
  'ip',
  'status',
  'method',
  'uri',
  'agent',
  'referrer',
  'xff',
  'length',
  'start',
  'end',
]
const ASCII = /^[\x00-\xFF]*$/

/**
 * check for cloudfront real-time log events, with known TSV fields
 */
exports.detect = function (buffer) {
  const str = buffer.toString('utf-8')
  return ASCII.test(str) && str.split('\t').length === FIELDS.length
}

/**
 * parse tab separated log lines
 */
exports.decode = async function (buffer) {
  const parts = buffer.toString('utf-8').trim().split('\t')
  const data = {}
  FIELDS.forEach((name, index) => {
    if (parts[index] === '-') {
      data[name] = null
    } else {
      data[name] = parts[index]
    }
  })

  // only 2XX GET requests count
  if (data.method !== 'GET' || data.status < '200' || data.status >= '300') {
    return null
  }

  // couple things to parse out
  const time = Math.round(parseFloat(data.timestamp) * 1000)
  const le = url.parse(data.uri, true).query.le
  const digest = (data.uri || '').split('/').slice(-2, -1)[0]
  if (!time || !le || !digest) {
    return null
  }

  // calculate actual byte range returned
  const length = parseInt(data.length, 10) || 0
  const start = parseInt(data.start, 10) || 0
  const end = parseInt(data.end, 10) || length - 1
  if (length < 1) {
    return null
  }

  // format to look like https://github.com/PRX/dovetail-bytes-lambda
  // NOTE: we're missing the "total" and "region" fields, but it turns out
  // those aren't actually used anywhere, so it's fine.
  return { time, le, digest, start, end }
}
