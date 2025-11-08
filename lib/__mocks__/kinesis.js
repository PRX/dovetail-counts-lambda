const kinesis = jest.createMockFromModule('../kinesis')
const actual = jest.requireActual('../kinesis')

process.env.KINESIS_IMPRESSION_STREAM = 'anything'

kinesis.__records = []
kinesis.__clearRecords = () => kinesis.__records = []

// use a fake _putRecords
actual._putRecords = async ({StreamName, Records}) => {
  Records.forEach(({Data}) => kinesis.__records.push(JSON.parse(Data)))
  return {Records: Records.map(r => ({}))}
}

// call actual put methods
kinesis.format = actual.format
kinesis.putWithLock = actual.putWithLock

module.exports = kinesis
