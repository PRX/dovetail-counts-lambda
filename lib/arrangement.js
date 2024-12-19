const log = require('lambda-log')
const {
  ArrangementNotFoundError,
  ArrangementInvalidError,
  ArrangementNoBytesError,
  ArrangementSkippableError,
} = require('./errors')
const dynamo = require('./dynamo')
const { promiseAnyTruthy } = require('./util')

const DEFAULT_TTL = 86400
const DEFAULT_INCOMPLETE_TTL = 300
const DEFAULT_BITRATE = 128000
const MEMOIZED = {}

/**
 * DDB arrangements, cached in redis
 */
module.exports = class Arrangement {
  constructor(digest, data) {
    this.digest = digest
    if (data && data.skip) {
      throw new ArrangementSkippableError(`Skipping ${digest}`)
    }
    if (!data || !data.version || !data.data) {
      throw new ArrangementInvalidError(`Invalid ${digest}`)
    }
    if (data.version < 3 || !data.data.b || !data.data.b.length) {
      throw new ArrangementNoBytesError(`Old ${digest}`)
    }
    if (data.version < 4 || !data.data.a) {
      log.warn('Non v4 arrangement', { digest }) // allow, but warn
    }
    this.version = data.version
    this.types = data.data.t
    this.bytes = data.data.b
    this.analysis = this.decodeAnalysis(data.data.a)
    if (this.types.length !== this.bytes.length - 1) {
      throw new ArrangementInvalidError(`Mismatch ${digest}`)
    }
    this.segments = this.types.split('').map((type, idx) => {
      return [this.bytes[idx], this.bytes[idx + 1] - 1]
    })
  }

  static async load(digest, redis, ddbClient) {
    if (MEMOIZED[digest]) {
      return MEMOIZED[digest]
    } else {
      return (MEMOIZED[digest] = this._load(digest, redis, ddbClient).then(
        arr => {
          delete MEMOIZED[digest]
          return arr
        },
        err => {
          delete MEMOIZED[digest]
          throw err
        },
      ))
    }
  }

  static async _load(digest, redis, ddbClient) {
    const cached = await redis.getJson(`dtcounts:ddb:${digest}`)
    if (cached) {
      return new Arrangement(digest, cached)
    } else {
      const uncached = await dynamo.getArrangement(digest, ddbClient)
      if (uncached) {
        const arr = new Arrangement(digest, uncached)

        // shorter ttl for incomplete arrangements, in case we later re-stitch and get all files
        let ttl = process.env.REDIS_ARRANGEMENT_TTL || DEFAULT_TTL
        if (uncached.incomplete) {
          ttl = process.env.REDIS_ARRANGEMENT_INCOMPLETE_TTL || DEFAULT_INCOMPLETE_TTL
        }

        await redis.setex(`dtcounts:ddb:${digest}`, ttl, arr.encode())
        return arr
      } else {
        throw new ArrangementNotFoundError(`Missing ${digest}`)
      }
    }
  }

  get bitrate() {
    const format = this.analysis && this.analysis.f

    let bitrate = 0
    if (format === 'wav') {
      bitrate = this.analysis.b * this.analysis.c * this.analysis.s
    } else if (format === 'flac') {
      // oof - no real way to know. let's guess, assuming a 2:1 compression!
      bitrate = Math.round((this.analysis.b * this.analysis.c * this.analysis.s) / 2)
    } else if (format === 'mp3') {
      bitrate = this.analysis.b * (this.analysis.b <= 320 ? 1000 : 1)
    }

    if (bitrate > 0) {
      return bitrate
    } else {
      return parseInt(process.env.DEFAULT_BITRATE) || DEFAULT_BITRATE
    }
  }

  get percentAds() {
    let adBytes = 0
    let totalBytes = this.segmentSize()

    this.types.split('').forEach((type, idx) => {
      if (type !== 'o' && type !== 'i') {
        adBytes += this.segmentSize(idx)
      }
    })

    return totalBytes ? adBytes / totalBytes : 0
  }

  // log all non-original segments
  isLoggable(idx) {
    return !!this.types[idx] && this.types[idx] !== 'o'
  }

  encode() {
    const data = { t: this.types, b: this.bytes, a: this.analysis }
    return JSON.stringify({ version: this.version, data })
  }

  decodeAnalysis(analysis) {
    if (Array.isArray(analysis) && analysis.length === 3) {
      return { f: 'mp3', b: analysis[0], c: analysis[1], s: analysis[2] }
    } else if (analysis && analysis.f) {
      return analysis
    } else {
      return null
    }
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

  segmentPosition(idx) {
    if (this.types[idx] === 'o') {
      return null
    }
    const prevTypes = this.types.substr(0, idx)
    const prevAds = prevTypes.split('o').pop()
    return prevAds.length
  }

  bytesToSeconds(numBytes) {
    return numBytes / (this.bitrate / 8)
  }

  bytesToPercent(numBytes, segmentIdx) {
    return numBytes / this.segmentSize(segmentIdx)
  }
}
