const s3 = jest.genMockFromModule('../s3')

let ARRANGEMENTS = {}
s3.__addArrangement = (digest, arr) => ARRANGEMENTS[digest] = arr
s3.__clearArrangements = () => ARRANGEMENTS = {}

s3.getArrangement = async function(digest) {
  return ARRANGEMENTS[digest] || null
}

module.exports = s3
