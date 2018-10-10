let PUT_DIGESTS = [], PUT_IMPRESSIONS = []

exports.__clearRecords = () => {
  PUT_DIGESTS = []
  PUT_IMPRESSIONS = []
}
exports.__getDigests = () => PUT_DIGESTS
exports.__getImpressions = () => PUT_IMPRESSIONS

exports.putMissingDigest = async function(digest) {
  PUT_DIGESTS.push(digest)
  return true
}

exports.putBigQuery = async function(data) {
  PUT_IMPRESSIONS.push(data)
  return true
}
