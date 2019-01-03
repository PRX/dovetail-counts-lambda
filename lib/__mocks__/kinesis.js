const kinesis = jest.genMockFromModule('../kinesis')

kinesis.__records = []
kinesis.__clearRecords = () => kinesis.__records = []

// call actual put methods
kinesis.putMissingDigest = (digest) => {
  kinesis.__records.push(digest)
  return true
}
kinesis.putImpression = (data) => {
  kinesis.__records.push(data)
  return true
}
kinesis.putImpressionLock = (redis, data) => {
  return kinesis.putImpression(data)
}

module.exports = kinesis
