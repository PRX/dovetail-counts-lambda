const kinesis = jest.genMockFromModule('../kinesis')

process.env.KINESIS_ARRANGEMENT_STREAM = 'anything'
process.env.KINESIS_IMPRESSION_STREAM = 'anything'

kinesis.__records = []
kinesis.__clearRecords = () => kinesis.__records = []
kinesis.putRecord = require.requireActual('../kinesis').putRecord = async (stream, data) => {
  kinesis.__records.push(data)
  return true
}

// call actual put methods
kinesis.putMissingDigest = require.requireActual('../kinesis').putMissingDigest
kinesis.putImpression = require.requireActual('../kinesis').putImpression
kinesis.putImpressionLock = require.requireActual('../kinesis').putImpressionLock

module.exports = kinesis
