const { ExtendableError } = require('./errors')

describe('errors', () => {

  class TestError extends ExtendableError {}

  it('correctly extends error', () => {
    const err = new TestError('something went wrong') // line 8
    expect(err).toBeInstanceOf(TestError)
    expect(err.name).toEqual('TestError')
    expect(err.message).toEqual('something went wrong')
    expect(err.stack).toMatch(/^TestError:/)
    expect(err.stack).not.toMatch('lib/errors.js')
    expect(err.stack).toMatch('lib/errors.test.js:8')
  })

  it('includes an original error', () => {
    const original = new Error('something went wrong') // line 18
    const err = new TestError(original)
    expect(err).toBeInstanceOf(TestError)
    expect(err.name).toEqual('TestError')
    expect(err.message).toEqual('something went wrong')
    expect(err.stack).toMatch(/^TestError:/)
    expect(err.stack).not.toMatch('lib/errors.js')
    expect(err.stack).toMatch('lib/errors.test.js:18')
  })

  it('overrides an original error message', () => {
    const original = new Error('something went wrong') // line 29
    const err = new TestError('huh?', original)
    expect(err).toBeInstanceOf(TestError)
    expect(err.name).toEqual('TestError')
    expect(err.message).toEqual('huh? - something went wrong')
    expect(err.stack).toMatch(/^TestError:/)
    expect(err.stack).not.toMatch('lib/errors.js')
    expect(err.stack).toMatch('lib/errors.test.js:29')
  })

})
