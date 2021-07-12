/**
 * Resolve to the 1st successful truthy response from a promise
 */
exports.promiseAnyTruthy = (p1, p2) => {
  return new Promise((resolve, reject) => {
    let truthyCount = 0
    let otherCount = 0

    const success = result => {
      if (result && truthyCount++ === 0) {
        resolve(result)
      } else if (!result && otherCount++ === 1) {
        resolve(result)
      }
    }
    const failure = err => {
      if (otherCount++ === 1) {
        reject(err)
      }
    }

    p1.then(success, failure)
    p2.then(success, failure)
  })
}
