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
    const txn = this.env.beginTxn({ readOnly: true })
    try {
      const data = this.deserialize(txn.getStringUnsafe(this.dbi, soul))
      txn.commit()
      return data
    } catch (e) {
      txn.abort()
      throw e
    }
  }

  getRaw(soul: string) {
    if (!soul) return null
    const txn = this.env.beginTxn({ readOnly: true })
    try {
      const data = txn.getString(this.dbi, soul)
      txn.commit()
      return data || ''
    } catch (e) {
      txn.abort()
      throw e
    }
  }

  serialize(node: GunNode) {
    return JSON.stringify(node)
  }

  deserialize(data: string) {
    return JSON.parse(data)
  }

  writeNode(soul: string, nodeData: GunNode) {
    if (!soul) return
    const txn = this.env.beginTxn()

    try {
      const existingData = txn.getStringUnsafe(this.dbi, soul)
      const node = this.deserialize(existingData) || undefined
      const existingGraph = { [soul]: node }
      const graphUpdates = { [soul]: nodeData }
      const graphDiff = diffGunCRDT(graphUpdates, existingGraph)
      if (!graphDiff || !graphDiff[soul]) return
      const updatedGraph = mergeGraph(existingGraph, graphDiff)
      const updated = updatedGraph[soul]
      txn.putString(this.dbi, soul, this.serialize(updated!))
      txn.commit()
      return graphDiff[soul]
    } catch (e) {
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
