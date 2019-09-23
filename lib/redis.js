const IORedis = require('ioredis');
const log = require('lambda-log')
const { promisify } = require('util')
const { MissingEnvError, RedisConnError } = require('./errors')

/**
 * Redis connection (lambdas have to connect/quit on every execution)
 */
module.exports = class Redis {

  constructor(redisUrl, maxRetries = 5) {
    this.disconnected = true

    const opts = {
      maxRetriesPerRequest: maxRetries,
      retryStrategy: times => {
        if (times <= maxRetries) {
          return Math.min(times * 50, 2000)
        } else {
          this.disconnected = true
          return null
        }
      },
    }

    // optionally run in cluster mode
    if (redisUrl.match(/^cluster:\/\/.+:[0-9]+$/)) {
      const parts = redisUrl.replace(/^cluster:\/\//, '').split(':')
      this.conn = new IORedis.Cluster([{host: parts[0], port: parts[1]}], {redisOptions: opts})
    } else {
      this.conn = new IORedis(redisUrl, opts)
    }
    this.conn.on('connect', () => this.disconnected = false)
    this.conn.on('error', err => {
      log.error(new RedisConnError(err))
      this.disconnected = true
    })

    // custom push-onto-list function
    this.conn.defineCommand('pushlist', {
      numberOfKeys: 1,
      lua: `
        local val = redis.call('GET', KEYS[1])
        if val then val = val .. ',' .. ARGV[1] else val = ARGV[1] end
        if tonumber(ARGV[2]) > 0 then
          redis.call('SETEX', KEYS[1], ARGV[2], val)
        else
          redis.call('SET', KEYS[1], val)
        end
        return val
      `.replace(/\n/g, '').replace(/ +/g, ' ').trim()
    })
  }

  get connected() {
    return !this.disconnected
  }

  async disconnect() {
    try {
      this.disconnected = true
      await this.conn.quit()
      return true
    } catch (err) {
      console.error('disconnect err:', err)
      return false
    }
  }

  get() {
    return this.cmd('get', arguments)
  }

  async getJson() {
    const str = await this.cmd('get', arguments)
    if (str) {
      try {
        return JSON.parse(str)
      } catch (e) {
        return null
      }
    } else {
      return str
    }
  }

  set() {
    return this.cmd('set', arguments)
  }

  setex() {
    return this.cmd('setex', arguments)
  }

  del() {
    return this.cmd('del', arguments)
  }

  async lockValue(key, val, ttl) {
    let setter
    if (ttl) {
      setter = ['set', key, val, 'nx', 'ex', ttl]
    } else {
      setter = ['setnx', key, val]
    }
    const res = await this.multi([setter, ['get', key]])
    return res[0][1] === 1 || res[1][1] === val
  }

  async lock(key, field, ttl) {
    const res = await this.cmd('hsetnx', key, field, '')
    if (res === 1) {
      if (ttl) {
        await this.cmd('expire', key, ttl)
      }
      return true
    } else {
      return false
    }
  }

  async nuke(pattern) {
    const keys = await this.cmd('keys', [pattern])
    return Promise.all(keys.map(k => this.del(k)))
  }

  ttl() {
    return this.cmd('ttl', arguments)
  }

  eval() {
    return this.cmd('eval', arguments)
  }

  push(key, strOrArray, ttl = 0) {
    if (typeof(strOrArray) !== 'string') {
      strOrArray = strOrArray.join(',')
    }
    if (strOrArray.length) {
      return this.cmd('pushlist', key, strOrArray, ttl)
    } else {
      return this.get(key)
    }
  }

  async cmd(method) {
    if (this.disconnected) {
      this.conn.connect().catch(err => null)
    }
    const args = Array.from(arguments).slice(1)
    try {
      return await this.conn[method].apply(this.conn, args)
    } catch (err) {
      throw new RedisConnError(err)
    }
  }

  async multi(cmds) {
    if (this.disconnected) {
      this.conn.connect().catch(err => null)
    }
    try {
      return await this.conn.multi(cmds).exec()
    } catch (err) {
      throw new RedisConnError(err)
    }
  }

}
