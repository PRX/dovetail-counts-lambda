const url = require('url')

// https://github.com/PRX/Infrastructure/blob/master/cdn/dovetail3-cdn.yml#L289
const FIELDS = [
  'timestamp',
  'c-ip',
  'sc-status',
  'cs-method',
  'cs-uri-stem',
  'cs-user-agent',
  'cs-referer',
  'x-forwarded-for',
  'sc-content-len',
  'sc-range-start',
  'sc-range-end',
]
const ASCII = /^[\x00-\xFF]*$/
const CLOUDFRONT_NULL = '-'

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
    if (parts[index] === CLOUDFRONT_NULL) {
      data[name] = null
    } else {
      data[name] = parts[index]
    }
  })

  // only 2XX GET requests count
  if (data['cs-method'] !== 'GET' || !data['sc-status'].match(/^2[0-9][0-9]$/)) {
    return null
  }

  // timestamps come in as `<epoch>.<milliseconds>` (ex: 1625753146.604)
  const time = Math.round(parseFloat(data.timestamp) * 1000)

  // uri stem includes query params (ex: /123/<guid>/<digest>/file.mp3?le=<le>)
  const parsed = url.parse(data['cs-uri-stem'] || '', true)
  const le = parsed.query.le

  // but digest is always 2nd to last (trimming trailing slashes)
  const digest = parsed.pathname.replace(/\/+$/, '').split('/').slice(-2, -1)[0]
  if (!time || !le || !digest) {
    return null
  }

  // calculate actual byte range returned
  const length = parseInt(data['sc-content-len'], 10) || 0
  const start = parseInt(data['sc-range-start'], 10) || 0
  const end = parseInt(data['sc-range-end'], 10) || (start + length - 1)
  if (length < 1) {
    return null
  }

  // format to look like https://github.com/PRX/dovetail-bytes-lambda
  // NOTE: we're missing the "total" and "region" fields, but it turns out
  // those aren't actually used anywhere, so it's fine.
  return { time, le, digest, start, end }
}
