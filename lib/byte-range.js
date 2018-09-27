const DEFAULT_TTL = 86400
const DEFAULT_BITRATE = 'what-should-this-be'

/**
 * Byte range operations
 */
module.exports = class ByteRange {

  constructor(requestUuid, byteRangesString) {
    this.uuid = requestUuid
    this.bytes = ByteRange.decode(byteRangesString)
  }

  static async load(uuid, redis, addBytes = null) {
    const key = `dtcounts:bytes:${uuid}`
    if (addBytes) {
      const ttl = process.env.REDIS_BYTES_TTL || DEFAULT_TTL
      const str = await redis.push(key, addBytes, ttl)
      return new ByteRange(uuid, str)
    } else {
      const str = await redis.get(key)
      return new ByteRange(uuid, str)
    }
  }

  static decode(str) {
    if (!str || !str.length) {
      return []
    }

    // parse '123-456' strings to int arrays
    let decoded = str.split(',').map(b => {
      const parts = b.split('-')
      const from = parseInt(parts[0])
      const to = parseInt(parts[1])
      if (!isNaN(from) && !isNaN(to)) {
        return [from, to]
      }
    }).filter(b => b)

    // put lowest "from" bytes first
    let sorted = decoded.sort((a, b) => a[0] > b[0])

    // combine overlapping ranges
    let combined = [sorted.shift()]
    sorted.forEach(nextRange => {
      const last = combined.length - 1
      if (combined[last][1] >= (nextRange[0] - 1)) {
        if (nextRange[1] > combined[last][1]) {
          combined[last][1] = nextRange[1]
        }
      } else {
        combined.push(nextRange)
      }
    })

    return combined
  }

  encode() {
    return this.bytes.map(b => `${b[0]}-${b[1]}`).join(',')
  }

}
