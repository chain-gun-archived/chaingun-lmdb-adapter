import lmdb from 'node-lmdb'

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
    const nodeDataMeta = (nodeData && nodeData['_']) || {}
    const nodeDataState = nodeDataMeta['>'] || {}

    try {
      const existingData = txn.getStringUnsafe(this.dbi, soul)
      const node = this.deserialize(existingData) || {}
      const meta = (node['_'] = node['_'] || { '#': soul, '>': {} })
      const state = (meta['>'] = meta['>'] || {})

      for (let key in nodeData) {
        if (key === '_' || !(key in nodeDataState)) continue
        node[key] = nodeData[key]
        state[key] = nodeDataState[key]
      }

      txn.putString(this.dbi, soul, this.serialize(node))
      txn.commit()
    } catch (e) {
      txn.abort()
      throw e
    }
  }

  write(put: GunPut) {
    if (!put) return
    for (let soul in put) this.writeNode(soul, put[soul])
  }

  close() {
    this.dbi.close()
    this.env.close()
  }
}
