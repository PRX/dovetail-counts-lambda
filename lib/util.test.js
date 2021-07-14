const util = require('./util')

describe('util', () => {
  describe('#promiseAnyTruthy', () => {
    it('resolves to the first returned value', async () => {
      const p1 = new Promise(resolve => setTimeout(() => resolve('p1'), 5))
      const p2 = new Promise(resolve => setTimeout(() => resolve('p2'), 1))
      const p3 = new Promise((_, reject) => setTimeout(() => reject('p3'), 1))
      expect(await util.promiseAnyTruthy(p1, p2, p3)).toEqual('p2')
    })

    it('checks for truthy values', async () => {
      const p1 = new Promise(resolve => setTimeout(() => resolve('p1'), 5))
      const p2 = new Promise(resolve => setTimeout(() => resolve(null), 1))
      expect(await util.promiseAnyTruthy(p1, p2)).toEqual('p1')
    })

    it('resolves to the last falsey value', async () => {
      const p1 = new Promise(resolve => setTimeout(() => resolve(''), 5))
      const p2 = new Promise(resolve => setTimeout(() => resolve(false), 1))
      const p3 = new Promise(resolve => setTimeout(() => resolve(null), 1))
      expect(await util.promiseAnyTruthy(p1, p2, p3)).toEqual('')
    })

    it('rejects to the last error promise', async () => {
      const p1 = new Promise((_, reject) => setTimeout(() => reject('p1'), 5))
      const p2 = new Promise((_, reject) => setTimeout(() => reject('p2'), 1))
      try {
        await util.promiseAnyTruthy(p1, p2)
        fail('should have gotten an error')
      } catch (err) {
        expect(err).toEqual('p1')
      }
    })

    it('picks a falsey value over an error', async () => {
      const p1 = new Promise(resolve => setTimeout(() => resolve(null), 1))
      const p2 = new Promise((_, reject) => setTimeout(() => reject('p2'), 5))
      const p3 = new Promise((_, reject) => setTimeout(() => reject('p3'), 5))
      const p4 = new Promise((_, reject) => setTimeout(() => reject('p4'), 5))
      expect(await util.promiseAnyTruthy(p1, p2, p3, p4)).toEqual(null)
    })
  })
})
