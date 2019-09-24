const log = require('lambda-log')
const DEFAULT_TTL = 86400

/**
 * Lock an impression (or "all") in redis
 */
exports.lock = async function(redis, record) {
  if (!record) {
    return null;
  }

  const key = lockKey(record)
  const fld = record.segment === undefined ? 'all' : record.segment
  if (await redis.lock(key, fld, ttl())) {
    return record
  } else {
    return null;
  }
}

/**
 * Unlock an impression (or "all") in redis w/NO ERRORS
 */
exports.unlock = async function(redis, record) {
  if (!record) {
    return null;
  }

  const key = lockKey(record)
  const fld = record.segment === undefined ? 'all' : record.segment
  try {
    await redis.unlock(key, fld)
    return true
  } catch (err) {
    log.warn(err, {key, fld, msg: 'Kinesis unlock failed'})
    return false
  }
}

/**
 * Lock a listener-episode to a digest
 */
exports.lockDigest = async function(redis, record) {
  if (!record) {
    return null;
  }

  if (await redis.lockValue(digestKey(record), record.digest, ttl())) {
    return record
  } else {
    return {isDuplicate: true, cause: 'digestCache', ...record}
  }
}

// key for a lock
function lockKey({timestamp, listenerEpisode, digest}) {
  const day = timeToDay(timestamp)
  return `dtcounts:imp:${listenerEpisode}:${day}:${digest}`
}

// key for a digest lock
function digestKey({timestamp, listenerEpisode}) {
  const day = timeToDay(timestamp)
  return `dtcounts:imp:${listenerEpisode}:${day}:digest`
}

// get the UTC day for an epoch time
function timeToDay(time) {
  return (time ? new Date(time) : new Date).toISOString().substr(0, 10)
}

// env configured redis ttl
function ttl() {
  return process.env.REDIS_IMPRESSION_TTL || DEFAULT_TTL
}
