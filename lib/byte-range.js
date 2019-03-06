const DEFAULT_TTL = 86400

/**
 * Byte range operations
 */
module.exports = class ByteRange {

  constructor(byteRangesString) {
    this.bytes = ByteRange.decode(byteRangesString)
  }

  static async load(id, redis, addBytes = null) {
    const key = `dtcounts:bytes:${id}`
    if (addBytes) {
      const ttl = process.env.REDIS_BYTES_TTL || DEFAULT_TTL
      const str = await redis.push(key, addBytes, ttl)
      return new ByteRange(str)
    } else {
      const str = await redis.get(key)
      return new ByteRange(str)
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

  intersect(firstByte, lastByte) {
    if (lastByte === undefined) {
      lastByte = firstByte[1]
      firstByte = firstByte[0]
    }
    return this.bytes.reduce((count, range) => {
      const lower = range[0]
      const upper = range[1]
      if (lower >= firstByte && lower <= lastByte) {
        return count + (Math.min(lastByte, upper) - lower + 1)
      } else if (upper >= firstByte && upper <= lastByte) {
        return count + (upper - Math.max(firstByte, lower) + 1)
      } else if (lower <= firstByte && upper >= lastByte) {
        return count + (lastByte - firstByte + 1)
      } else {
        return count
      }
    }, 0)
  }

  complete(firstByte, lastByte) {
    return this.intersect(firstByte, lastByte) > (lastByte - firstByte)
  }

  total() {
    return this.bytes.reduce((count, range) => {
      return count + (range[1] - range[0] + 1)
    }, 0)
  }

}
