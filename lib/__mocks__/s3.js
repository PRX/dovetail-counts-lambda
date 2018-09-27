let MOCKS = {}

exports.getArrangement = async function(digest) {
  return MOCKS[digest] || null
}

exports.mockS3 = function(key, value) {
  if (value) {
    MOCKS[key] = value
  } else {
    delete MOCKS[key]
  }
}

exports.mockClear = function() {
  MOCKS = {}
}
