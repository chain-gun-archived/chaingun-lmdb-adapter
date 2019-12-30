import { diffGunCRDT, mergeGraph } from '@chaingun/crdt'
import {
  GunGetOpts,
  GunGraphAdapter,
  GunGraphData,
  GunNode,
  GunValue
} from '@chaingun/types'
import lmdb from 'node-lmdb'

const DEFAULT_DB_NAME = 'gun-nodes'
const WIDE_NODE_MARKER = 'WIDE_NODE'
const WIDE_NODE_THRESHOLD =
  parseInt(process.env.GUN_LMDB_WIDE_NODE_THRESHOLD || '', 10) || 1100
const GET_MAX_KEYS =
  parseInt(process.env.GUN_LMDB_GET_MAX_KEYS || '', 10) || 10000

const DEFAULT_CRDT_OPTS = {
  diffFn: diffGunCRDT,
  mergeFn: mergeGraph
}

type LmdbOptions = any
type LmdbEnv = any
type LmdbDbi = any
type LmdbTransaction = any

export function wideNodeKey(soul: string, key = ''): string {
  return `wide:${soul}/${key}`
}

/**
 * Open a LMDB database as a Gun Graph Adapter
 *
 * @param opts same opts as node-lmdb.Env.open
 * @param name database name, defaults to "gun-nodes"
 * @returns a GunGraphAdapter that reads/writes to the LMDB database
 */
export function createGraphAdapter(
  opts: LmdbOptions,
  name = DEFAULT_DB_NAME
): GunGraphAdapter {
  const [env, dbi] = openEnvAndDbi(opts, name)
  return adapterFromEnvAndDbi(env, dbi)
}

/**
 * Create Gun Graph Adapter from open LMDB database
 *
 * @param env lmdb.Env object
 * @param dbi lmdb DBI object
 * @returns a GunGraphAdapter that reads/writes to the LMDB database
 */
export function adapterFromEnvAndDbi(
  env: lmdb.Env,
  dbi: LmdbDbi
): GunGraphAdapter {
  return {
    close: () => {
      env.close()
      dbi.close()
    },
    get: (soul: string, opts?: GunGetOpts) => get(env, dbi, soul, opts),
    getJsonString: (soul: string, opts?: GunGetOpts) =>
      getJsonString(env, dbi, soul, opts),
    getJsonStringSync: (soul: string, opts?: GunGetOpts) =>
      getJsonStringSync(env, dbi, soul, opts),
    getSync: (soul: string, opts?: GunGetOpts) => getSync(env, dbi, soul, opts),
    put: (graphData: GunGraphData) => put(env, dbi, graphData),
    putSync: (graphData: GunGraphData) => putSync(env, dbi, graphData)
  }
}

export function readWideNode(
  env: LmdbEnv,
  dbi: LmdbDbi,
  soul: string,
  opts?: GunGetOpts
): GunNode {
  return transaction<GunNode>(env, txn => {
    const stateVectors: Record<string, number> = {}
    const node: any = {
      _: {
        '#': soul,
        '>': stateVectors
      }
    }
    const cursor = new lmdb.Cursor(txn, dbi)
    const singleKey = opts && opts['.']
    const lexStart = (opts && opts['>']) || singleKey
    const lexEnd = (opts && opts['<']) || singleKey
    // tslint:disable-next-line: no-let
    let keyCount = 0

    try {
      const base = wideNodeKey(soul)
      const startKey = lexStart
        ? wideNodeKey(soul, lexStart)
        : wideNodeKey(soul)
      // tslint:disable-next-line: no-let
      let dbKey = cursor.goToRange(startKey)

      if (dbKey === startKey && lexStart && !singleKey) {
        // Exclusive lex?
        dbKey = cursor.goToNext()
      }

      while (dbKey && dbKey.indexOf(base) === 0) {
        const key = dbKey.replace(base, '')

        if (lexEnd && key > lexEnd) {
          break
        }

        const { stateVector, value } = readWideNodeKey(dbi, txn, soul, key)

        if (stateVector) {
          // tslint:disable-next-line: no-object-mutation
          stateVectors[key] = stateVector
          // tslint:disable-next-line: no-object-mutation
          node[key] = value
          keyCount++
        }

        dbKey = cursor.goToNext()

        if (keyCount > GET_MAX_KEYS || (lexEnd && key === lexEnd)) {
          break
        }
      }
    } catch (e) {
      throw e
    } finally {
      cursor.close()
    }

    return keyCount ? node : null
  })
}

/**
 * Load Gun Node data from a LMDB database synchronously
 *
 * @param env lmdb.Env object
 * @param dbi lmdb DBI object
 * @param soul the unique identifier of the node to fetch
 */
export function getSync(
  env: LmdbEnv,
  dbi: LmdbDbi,
  soul: string,
  opts?: GunGetOpts
): GunNode | null {
  if (!soul) {
    return null
  }

  return transaction(
    env,
    txn => {
      const raw = txn.getStringUnsafe(dbi, soul)

      if (raw === WIDE_NODE_MARKER) {
        return readWideNode(env, dbi, soul, opts)
      }

      const node = deserialize(raw)

      if (node && opts) {
        const singleKey = opts && opts['.']
        const lexStart = (opts && opts['>']) || singleKey
        const lexEnd = (opts && opts['<']) || singleKey

        if (!(lexStart || lexEnd)) {
          return node
        }

        const resultState: Record<string, number> = {}
        const result: any = {
          _: {
            '#': soul,
            '>': resultState
          }
        }

        const state = node._['>']
        // tslint:disable-next-line: no-let
        let keyCount = 0
        Object.keys(state).forEach(key => {
          if (
            lexStart &&
            key >= lexStart &&
            lexEnd &&
            key <= lexEnd &&
            key in state
          ) {
            // tslint:disable-next-line: no-object-mutation
            result[key] = node[key]
            // tslint:disable-next-line: no-object-mutation
            resultState[key] = state[key]
            keyCount++
          }
        })

        return keyCount ? result : null
      }

      return node
    },
    {
      readOnly: true
    }
  )
}

/**
 * Load Gun Node data from a LMDB database asynchronously
 *
 * @param env lmdb.Env object
 * @param dbi lmdb DBI object
 * @param soul the unique identifier of the node to fetch
 */
export async function get(
  env: LmdbEnv,
  dbi: LmdbDbi,
  soul: string,
  opts?: GunGetOpts
): Promise<GunNode | null> {
  return getSync(env, dbi, soul, opts)
}

/**
 * Load Gun Node data as a string from a LMDB database synchronously
 *
 * @param env lmdb.Env object
 * @param dbi lmdb DBI object
 * @param soul the unique identifier of the node to fetch
 */
export function getJsonStringSync(
  env: LmdbEnv,
  dbi: LmdbDbi,
  soul: string,
  opts?: GunGetOpts
): string {
  if (!soul) {
    return ''
  }

  if (opts) {
    return JSON.stringify(getSync(env, dbi, soul, opts))
  }

  return transaction(
    env,
    txn => {
      const raw = txn.getString(dbi, soul) || ''

      if (raw === WIDE_NODE_MARKER) {
        return JSON.stringify(readWideNode(env, dbi, soul))
      }

      return raw
    },
    {
      readOnly: true
    }
  )
}

/**
 * Load Gun Node data as a string from a LMDB database asynchronously
 *
 * @param env lmdb.Env object
 * @param dbi lmdb DBI object
 * @param soul the unique identifier of the node to fetch
 */
export async function getJsonString(
  env: LmdbEnv,
  dbi: LmdbDbi,
  soul: string,
  opts?: GunGetOpts
): Promise<string> {
  return getJsonStringSync(env, dbi, soul, opts)
}

export function putNode(
  dbi: LmdbDbi,
  txn: LmdbTransaction,
  soul: string,
  node: GunNode | undefined,
  updated: GunNode,
  opts = DEFAULT_CRDT_OPTS
): GunNode | null {
  const { diffFn = diffGunCRDT, mergeFn = mergeGraph } = opts
  const existingGraph = { [soul]: node }
  const graphUpdates = { [soul]: updated }
  const graphDiff = diffFn(graphUpdates, existingGraph)
  const nodeDiff = graphDiff && graphDiff[soul]
  if (!nodeDiff || !graphDiff) {
    return null
  }

  const updatedGraph = mergeFn(existingGraph, graphDiff)
  const result = updatedGraph[soul]

  if (result && Object.keys(result).length >= WIDE_NODE_THRESHOLD) {
    // tslint:disable-next-line: no-console
    console.log('converting to wide node', soul)
    txn.putString(dbi, soul, WIDE_NODE_MARKER)
    putWideNode(dbi, txn, soul, result, opts)
  } else {
    // tslint:disable-next-line: no-expression-statement
    txn.putString(dbi, soul, serialize(result!))
  }

  return nodeDiff
}

export function readWideNodeKey(
  dbi: LmdbDbi,
  txn: LmdbTransaction,
  soul: string,
  key: string
): {
  readonly stateVector?: number
  readonly value?: GunValue
} {
  const dbKey = wideNodeKey(soul, key)
  const raw = txn.getStringUnsafe(dbi, dbKey)

  if (!raw) {
    return {
      stateVector: undefined,
      value: undefined
    }
  }

  const { stateVector, value } = JSON.parse(raw) || {}

  return {
    stateVector,
    value
  }
}

export function putWideNode(
  dbi: LmdbDbi,
  txn: LmdbTransaction,
  soul: string,
  updated: GunNode,
  opts = DEFAULT_CRDT_OPTS
): GunNode | null {
  const { diffFn = diffGunCRDT } = opts
  const stateVectors: Record<string, number> = {}
  const existingNode: any = {
    _: {
      '#': soul,
      '>': stateVectors
    }
  }

  for (const key in updated) {
    if (!key) {
      continue
    }

    const { stateVector, value } = readWideNodeKey(dbi, txn, soul, key)

    if (stateVector) {
      // tslint:disable-next-line: no-object-mutation
      stateVectors[key] = stateVector
      // tslint:disable-next-line: no-object-mutation
      existingNode[key] = value
    }
  }

  const existingGraph = { [soul]: existingNode }
  const graphUpdates = { [soul]: updated }
  const graphDiff = diffFn(graphUpdates, existingGraph)
  const nodeDiff = graphDiff && graphDiff[soul]

  if (!nodeDiff) {
    return null
  }

  for (const key in nodeDiff) {
    if (!key) {
      continue
    }

    txn.putString(
      dbi,
      wideNodeKey(soul, key),
      JSON.stringify({
        stateVector: nodeDiff._['>'][key],
        value: nodeDiff[key]
      })
    )
  }

  return nodeDiff
}

/**
 * Write Gun Graph data to the LMDB database synchronously
 *
 * @param env lmdb.Env object
 * @param dbi lmdb DBI object
 * @param graphData the Gun Graph data to write
 * @param opts
 */
export function putSync(
  env: LmdbEnv,
  dbi: LmdbDbi,
  graphData: GunGraphData,
  opts = DEFAULT_CRDT_OPTS
): GunGraphData | null {
  if (!graphData) {
    return null
  }

  const diff: GunGraphData = {}
  // tslint:disable-next-line: no-let
  let hasDiff = false

  return transaction(env, txn => {
    for (const soul in graphData) {
      if (!soul || !graphData[soul]) {
        continue
      }

      const existingData = txn.getStringUnsafe(dbi, soul)

      // tslint:disable-next-line: no-let
      let nodeDiff = null

      if (existingData === WIDE_NODE_MARKER) {
        nodeDiff = putWideNode(dbi, txn, soul, graphData[soul]!, opts)
      } else {
        const node = deserialize(existingData) || undefined
        nodeDiff = putNode(dbi, txn, soul, node, graphData[soul]!, opts)
      }

      if (nodeDiff) {
        // @ts-ignore
        // tslint:disable-next-line
        diff[soul] = nodeDiff
        // tslint:disable-next-line: no-expression-statement
        hasDiff = true
      }
    }

    return hasDiff ? diff : null
  })
}

/**
 * Write Gun Graph data to the LMDB database asynchronously
 *
 * @param env lmdb.Env object
 * @param dbi lmdb DBI object
 * @param graphData the Gun Graph data to write
 */
export async function put(
  env: LmdbEnv,
  dbi: LmdbDbi,
  graphData: GunGraphData
): Promise<GunGraphData | null> {
  return putSync(env, dbi, graphData)
}

/**
 * Open a LMDB database
 *
 * @param opts same opts as node-lmdb.Env.open
 * @param name name of the LMDB database to open (defaults to "gun-nodes")
 */
export function openEnvAndDbi(
  opts: LmdbOptions,
  name = DEFAULT_DB_NAME
): readonly [LmdbEnv, LmdbDbi] {
  const env = new lmdb.Env()
  // tslint:disable-next-line: no-expression-statement
  env.open(opts)
  const dbi = env.openDbi({
    create: true,
    name
  })

  return [env, dbi]
}

/**
 * Execute a transaction on a LMDB database
 *
 * @param env lmdb.Env object
 * @param fn This function is passed the transaction and is expected to return synchronously
 * @param opts options for the LMDB transaction passed to beginTxn
 */
export function transaction<T = any>(
  env: LmdbEnv,
  fn: (txn: LmdbTransaction) => T,
  opts?: any
): T {
  const txn: LmdbTransaction = env.beginTxn(opts)
  // tslint:disable-next-line: no-let
  let result: T
  try {
    result = fn(txn)
    txn.commit()
    return result
  } catch (e) {
    // tslint:disable-next-line: no-console
    console.error('lmdb transaction error', e.stack || e)
    txn.abort()
    throw e
  }
}

/**
 * Serialize Gun Node data for writing to LMDB database
 *
 * @param node the GunNode to serialize
 */
export function serialize(node: GunNode): string {
  return JSON.stringify(node)
}

/**
 * Deserialize GunNode data read from the LMDB database
 *
 * @param data the string data to parse as a GunNode
 */
export function deserialize(data: string): GunNode {
  return JSON.parse(data)
}
