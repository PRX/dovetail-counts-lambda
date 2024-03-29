const url = require('url')

// cloudfront access logs v1.0
// https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html#LogFileFormat
const FIELDS = [
  'date',
  'time',
  'x-edge-location',
  'sc-bytes',
  'c-ip',
  'cs-method',
  'cs(Host)',
  'cs-uri-stem',
  'sc-status',
  'cs(Referer)',
  'cs(User-Agent)',
  'cs-uri-query',
  'cs(Cookie)',
  'x-edge-result-type',
  'x-edge-request-id',
  'x-host-header',
  'cs-protocol',
  'cs-bytes',
  'time-taken',
  'x-forwarded-for',
  'ssl-protocol',
  'ssl-cipher',
  'x-edge-response-result-type',
  'cs-protocol-version',
  'fle-status',
  'fle-encrypted-fields',
  'c-port',
  'time-to-first-byte',
  'x-edge-detailed-result-type',
  'sc-content-type',
  'sc-content-len',
  'sc-range-start',
  'sc-range-end',
]
const ASCII = /^[\x00-\xFF]*$/
const CLOUDFRONT_NULL = '-'

/**
 * check for cloudfront standard log events, with known TSV fields
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

  // times and dates are split into 2 fields (ex: 2021-07-07 and 14:32:01)
  const time = new Date(`${data.date}T${data.time}Z`).valueOf()

  // query params are in their own field (ex: le=<le>)
  const le = url.parse('?' + (data['cs-uri-query'] || ''), true).query.le

  // uri stem does not include query params (ex: /123/<guid>/<digest>/file.mp3)
  const digest = (data['cs-uri-stem'] || '').replace(/\/+$/, '').split('/').slice(-2, -1)[0]
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
