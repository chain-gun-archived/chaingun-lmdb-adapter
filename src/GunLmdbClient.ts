import lmdb from 'node-lmdb'
import { diffGunCRDT, mergeGraph } from '@notabug/chaingun'

export const DEFAULT_CONFIG: LmdbOptions = {
  path: 'lmdb'
}

export class GunLmdbClient {
  env: any
  dbi: any
  lmdb: any

  constructor(lmdbConfig = DEFAULT_CONFIG) {
    this.lmdb = lmdb
    this.env = new lmdb.Env()
    this.env.open(lmdbConfig)
    this.dbi = this.env.openDbi({
      name: 'gun-nodes',
      create: true
    })
  }

  get(soul: string) {
    if (!soul) return null
    return this.transaction(txn => this.deserialize(txn.getStringUnsafe(this.dbi, soul)), {
      readOnly: true
    })
  }

  getRaw(soul: string) {
    if (!soul) return null
    return this.transaction(txn => txn.getString(this.dbi, soul) || '', {
      readOnly: true
    })
  }

  serialize(node: GunNode) {
    return JSON.stringify(node)
  }

  deserialize(data: string) {
    return JSON.parse(data)
  }

  writeNode(soul: string, nodeData: GunNode) {
    if (!soul) return
    return this.transaction(txn => {
      const existingData = txn.getStringUnsafe(this.dbi, soul)
      const node = this.deserialize(existingData) || undefined
      const existingGraph = { [soul]: node }
      const graphUpdates = { [soul]: nodeData }
      const graphDiff = diffGunCRDT(graphUpdates, existingGraph)
      if (!graphDiff || !graphDiff[soul]) return
      const updatedGraph = mergeGraph(existingGraph, graphDiff)
      const updated = updatedGraph[soul]
      txn.putString(this.dbi, soul, this.serialize(updated!))
      return graphDiff[soul]
    })
  }

  transaction(fn: (txn: any) => any, opts?: any) {
    const txn = this.env.beginTxn(opts)
    let result: any
    try {
      result = fn(txn)
      txn.commit()
      return result
    } catch (e) {
      console.error('transaction error', e.stack)
      txn.abort()
      throw e
    }
  }

  write(put: GunPut) {
    if (!put) return
    const diff: GunGraphData = {}
    let hasDiff = false

    for (let soul in put) {
      const nodeDiff = this.writeNode(soul, put[soul])
      if (nodeDiff) {
        diff[soul] = nodeDiff
        hasDiff = true
      }
    }

    if (hasDiff) return diff
  }

  close() {
    this.dbi.close()
    this.env.close()
  }
}
