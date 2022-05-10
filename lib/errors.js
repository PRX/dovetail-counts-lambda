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
        this.stack = new Error(message).stack
      }
    }
    this.name = this.constructor.name
  }
}
exports.ExtendableError = ExtendableError

/**
 * Things that should be retried (via kinesis)
 */
class RetryableError extends ExtendableError {
  constructor(msg, originalError) {
    super(msg, originalError)
    this.retryable = true
  }
}
exports.RetryableError = RetryableError

class SkippableError extends ExtendableError {
  constructor(msg, originalError) {
    super(msg, originalError)
    this.skippable = true
  }
}
exports.SkippableError = SkippableError

/**
 * Various named errors
 */
exports.ArrangementInvalidError = class ArrangementInvalidError extends SkippableError {}
exports.ArrangementNoBytesError = class ArrangementNoBytesError extends SkippableError {}
exports.ArrangementNotFoundError = class ArrangementNotFoundError extends ExtendableError {}
exports.ArrangementSkippableError = class ArrangementSkippableError extends SkippableError {}
exports.BadEventError = class BadEventError extends ExtendableError {}
exports.DynamodbGetError = class DynamodbGetError extends RetryableError {}
exports.KinesisPutError = class KinesisPutError extends RetryableError {
  constructor(msg, count) {
    super(msg)
    this.count = count
  }
}
exports.MissingEnvError = class MissingEnvError extends RetryableError {}
exports.RedisConnError = class RedisConnError extends RetryableError {}
