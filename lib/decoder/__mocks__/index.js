const decoder = jest.genMockFromModule('../index')

let BYTES = []
decoder.__addBytes = bytes => BYTES.push(bytes)
decoder.__clearBytes = () => (BYTES = [])

decoder.decodeEvent = async _event => decoder.formatResults(BYTES)

decoder.formatResults = jest.requireActual('../index').formatResults

module.exports = decoder
