const dynamo = jest.genMockFromModule('../dynamo')

let ARRANGEMENTS = {}
dynamo.__addArrangement = (digest, arr) => (ARRANGEMENTS[digest] = arr)
dynamo.__clearArrangements = () => (ARRANGEMENTS = {})

dynamo.getArrangement = async function (digest) {
  return ARRANGEMENTS[digest] || null
}

module.exports = dynamo
