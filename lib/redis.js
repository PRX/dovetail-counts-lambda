const redis = require('redis')
const promisify = require('util').promisify

/**
 * Redis connection (lambdas have to connect/quit on every execution)
 */
module.exports = class Redis {

  constructor(host) {
    this._host = process.env.REDIS_HOST || ''
    this._client = null
    this._pushFn = `
      local val = redis.call('GET', KEYS[1])
      if val then val = val .. ',' .. ARGV[1] else val = ARGV[1] end
      if tonumber(ARGV[2]) > 0 then
        redis.call('SETEX', KEYS[1], ARGV[2], val)
      else
        redis.call('SET', KEYS[1], val)
      end
      return val
    `.replace(/\n/g, '').replace(/ +/g, ' ').trim()
  }

  error(msg) {
    const err = new Error(`Redis error: ${msg}`)
    err.redis = true
    return err
  }

  get connected() {
    return this._client ? this._client.connected : false
  }

  connect() {
    if (this._client) {
      return Promise.resolve(this._client)
    } else {
      return new Promise((resolve, reject) => {
        const client = redis.createClient(this._opts(reject))
        client.on('error', err => reject(this.error(err)))
        client.on('connect', () => resolve(this._client = client))
      })
    }
  }

  disconnect() {
    if (this._client) {
      return new Promise((resolve, reject) => {
        this._client.on('error', err => {
          this._client = null
          reject(err)
        })
        this._client.on('end', () => {
          this._client = null
          resolve(true)
        })
        this._client.quit()
      })
    } else {
      return Promise.resolve(false)
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

  ttl() {
    return this.cmd('ttl', arguments)
  }

  eval() {
    return this.cmd('eval', arguments)
  }

  push(key, str, ttl = 0) {
    return this.eval(this._pushFn, 1, key, str, ttl).then(list => list.split(','))
  }

  async cmd(cmd, args) {
    const client = await this.connect()
    return promisify(client[cmd]).apply(client, args)
  }

  _opts(rejectFn) {
    const opts = {
      connect_timeout: 5000,
      retry_strategy: opts => {
        if (opts.error && opts.error.code === 'ECONNREFUSED') {
          rejectFn(this.error('ECONNREFUSED'))
        } else if (opts.attempt < 10) {
          return 100
        } else if (opts.error) {
          rejectFn(this.error(opts.error))
        } else {
          rejectFn(this.error(`failed after ${opts.attempt} attempts`))
        }
      }
    }
    if (this._host.match(/^redis:/)) {
      return {...opts, url: this._host}
    } else {
      return {...opts, host: this._host}
    }
  }

}
