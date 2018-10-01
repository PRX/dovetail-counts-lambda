const decoder = jest.genMockFromModule('../kinesis-decoder')

let BYTES = []
decoder.__addBytes = bytes => BYTES.push(bytes)
decoder.__clearBytes = () => BYTES = []

decoder.decodeEvent = async (_event) => decoder.formatResults(BYTES)

decoder.formatResults = require.requireActual('../kinesis-decoder').formatResults

module.exports = decoder
