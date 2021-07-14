/**
 * Resolve to the 1st successful truthy response from a promise
 */
exports.promiseAnyTruthy = (...promises) => {
  return new Promise((resolve, reject) => {
    const results = []
    const errors = []
    const allSettled = () => results.length + errors.length === promises.length

    // on success - resolve truthy values right away
    const success = result => {
      results.push(result)
      if (allSettled()) {
        resolve(result)
      } else if (results.filter(r => r).length === 1) {
        resolve(result)
      }
    }

    // on failure - wait until settled, then resolve any falsey result first
    const failure = err => {
      errors.push(err)
      if (allSettled()) {
        if (results.length) {
          resolve(results[results.length - 1])
        } else {
          reject(err)
        }
      }
    }

    // run all promises in parallel
    promises.forEach(p => p.then(success, failure))
  })
}
