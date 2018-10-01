const { ArrangementNotFoundError, ArrangementInvalidError, ArrangementNoBytesError } = require('./errors')
const s3 = require('./s3')

const DEFAULT_TTL = 86400
const DEFAULT_BITRATE = 128000
const MEMOIZED = {}

/**
 * S3 arrangements, cached in redis
 */
module.exports = class Arrangement {

  constructor(digest, data) {
    this.digest = digest
    if (!data || !data.version || !data.data) {
      throw new ArrangementInvalidError(`Invalid ${digest}`)
    }
    if (data.version !== 3 || !data.data.b || !data.data.b.length) {
      throw new ArrangementNoBytesError(`Old ${digest}`)
    }
    this.version = data.version
    this.types = data.data.t
    this.bytes = data.data.b
    if (this.types.length !== this.bytes.length - 1) {
      throw new ArrangementInvalidError(`Mismatch ${digest}`)
    }
    this.segments = this.types.split('').map((type, idx) => {
      if (idx === this.types.length - 1) {
        return [this.bytes[idx], this.bytes[idx + 1]]
      } else {
        return [this.bytes[idx], this.bytes[idx + 1] - 1]
      }
    })
  }

  static async load(digest, redis) {
    if (MEMOIZED[digest]) {
      return MEMOIZED[digest]
    } else {
      return MEMOIZED[digest] = this._load(digest, redis).then(
        arr => { delete MEMOIZED[digest]; return arr },
        err => { delete MEMOIZED[digest]; throw err }
      )
    }
  }

  static async _load(digest, redis) {
    const cached = await redis.getJson(`dtcounts:s3:${digest}`)
    if (cached) {
      return new Arrangement(digest, cached)
    } else {
      const fromS3 = await s3.getArrangement(digest)
      if (fromS3) {
        const arr = new Arrangement(digest, fromS3)
        const ttl = process.env.REDIS_ARRANGEMENT_TTL || DEFAULT_TTL
        await redis.setex(`dtcounts:s3:${digest}`, ttl, arr.encode())
        return arr
      } else {
        throw new ArrangementNotFoundError(`Missing ${digest}`)
      }
    }
  }

  encode() {
    const data = {t: this.types, b: this.bytes}
    return JSON.stringify({version: this.version, data})
  }

  segment(idx) {
    return this.segments[idx]
  }

  segmentSize(idx) {
    if (idx === undefined) {
      return this.segments[this.segments.length - 1][1] - this.segments[0][0] + 1
    } else {
      return this.segments[idx][1] - this.segments[idx][0] + 1
    }
  }

  // TODO: get actual bitrate from arrangement json
  bytesToSeconds(numBytes) {
    const bitsPerSecond = (parseInt(process.env.DEFAULT_BITRATE) || DEFAULT_BITRATE)
    return numBytes / (bitsPerSecond / 8)
  }

  bytesToPercent(numBytes, segmentIdx) {
    return numBytes / this.segmentSize(segmentIdx)
  }

}
