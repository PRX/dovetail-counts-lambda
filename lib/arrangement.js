const { ArrangementNotFoundError, ArrangementInvalidError, ArrangementNoBytesError } = require('./errors')
const s3 = require('./s3')
const DEFAULT_TTL = 86400

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
  }

  static async load(digest, redis) {
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

}
