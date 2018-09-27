const s3 = require('./s3')

/**
 * S3 arrangements, cached in redis
 */
module.exports = class Arrangement {

  constructor(data) {
    if (!data || !data.version || !data.data) {
      throw new Error('Invalid arrangement data')
    }
    if (data.version !== 3 || !data.data.b || !data.data.b.length) {
      throw new Error('Arrangement version < 3')
    }
    this.version = data.version
    this.types = data.data.t
    this.bytes = data.data.b
    if (this.types.length !== this.bytes.length - 1) {
      throw new Error('Arrangement segments/bytes mismatch')
    }
  }

  static async load(digest, redis) {
    const cached = await redis.getJson(`s3:arrangements:${digest}`)
    if (cached) {
      return new Arrangement(cached)
    } else {
      const fromS3 = await s3.getArrangement(digest)
      if (fromS3) {
        const arr = new Arrangement(fromS3)
        const ttl = process.env.REDIS_ARRANGEMENT_TTL || 86400
        await redis.setex(`s3:arrangements:${digest}`, ttl, arr.encode())
        return arr
      } else {
        throw new Error(`Arrangement ${digest} not in S3`)
      }
    }
  }

  encode() {
    const data = {t: this.types, b: this.bytes}
    return JSON.stringify({version: this.version, data})
  }

}
