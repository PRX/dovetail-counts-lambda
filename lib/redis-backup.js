const log = require('lambda-log')
const IORedis = require('ioredis')
const Redis = require('./redis')
const { MissingEnvError } = require('./errors')

/**
 * Wrapper for a redis connection, also sending SET commands to a backup redis
 */
module.exports = class RedisBackup extends Redis {

  constructor(redisUrl, backupUrl, maxRetries) {
    super(redisUrl, maxRetries)
    if (backupUrl) {
      this.backup = new Redis(backupUrl, maxRetries)
    }
  }

  async disconnect() {
    if (this.backup) {
      const results = await Promise.all([super.disconnect(), this.backup.disconnect()])
      return results[0]
    } else {
      return await super.disconnect()
    }
  }

  async cmd(method) {
    const args = Array.from(arguments)
    if (!this.backup || ['get', 'ttl', 'keys', 'scan'].indexOf(method) > -1) {
      return super.cmd.apply(this, args)
    } else {
      // TODO: hope that Promise.allSettled() exists someday
      const cmd1 = super.cmd.apply(this, args).then(r => ({value: r}), e => ({reason: e}))
      const cmd2 = this.backup.cmd.apply(this.backup, args).then(r => ({value: r}), e => ({reason: e}))

      const [result1, result2] = await Promise.all([cmd1, cmd2])
      if (result1.reason) {
        throw result1.reason
      } else if (result2.reason) {
        log.warn({err: result2.reason}, 'Backup redis failed')
        return result1.value
      } else {
        return result1.value
      }
    }
  }

  async multi(cmds) {
    if (!this.backup) {
      return super.multi(cmds)
    } else {
      // TODO: hope that Promise.allSettled() exists someday
      const cmd1 = super.multi(cmds).then(r => ({value: r}), e => ({reason: e}))
      const cmd2 = this.backup.multi(cmds).then(r => ({value: r}), e => ({reason: e}))

      const [result1, result2] = await Promise.all([cmd1, cmd2])
      if (result1.reason) {
        throw result1.reason
      } else if (result2.reason) {
        log.warn({err: result2.reason}, 'Backup redis multi failed')
        return result1.value
      } else {
        return result1.value
      }
    }
  }

}
