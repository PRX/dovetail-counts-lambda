/**
 * Extendable error
 */
class ExtendableError extends Error {
  constructor(msg, originalError = null) {
    if (originalError) {
      super(`${msg} - ${originalError.message}`)
      this.stack = originalError.stack.replace(/^.+:/, this.constructor.name + ':')
    } else if (!originalError && msg instanceof Error) {
      super(msg.message)
      this.stack = msg.stack.replace(/^.+:/, this.constructor.name + ':')
    } else {
      super(msg)
      if (typeof Error.captureStackTrace === 'function') {
        Error.captureStackTrace(this, this.constructor)
      } else {
        this.stack = (new Error(message)).stack
      }
    }
    this.name = this.constructor.name
  }
}
exports.ExtendableError = ExtendableError

/**
 * Various named errors
 */
exports.ArrangementInvalidError = class ArrangementInvalidError extends ExtendableError {}
exports.ArrangementNoBytesError = class ArrangementNoBytesError extends ExtendableError {}
exports.ArrangementNotFoundError = class ArrangementNotFoundError extends ExtendableError {}
exports.BadEventError = class BadEventError extends ExtendableError {}
exports.MissingEnvError = class MissingEnvError extends ExtendableError {}
exports.RedisConnError = class RedisConnError extends ExtendableError {}
