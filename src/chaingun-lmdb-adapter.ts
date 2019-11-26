import { diffGunCRDT, mergeGraph } from '@chaingun/crdt'
import { GunGraphAdapter, GunGraphData, GunNode } from '@chaingun/types'
import lmdb from 'node-lmdb'

const DEFAULT_DB_NAME = 'gun-nodes'
const DEFAULT_CRDT_OPTS = {
  diffFn: diffGunCRDT,
  mergeFn: mergeGraph
}

type LmdbOptions = any
type LmdbEnv = any
type LmdbDbi = any
type LmdbTransaction = any

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
    get: (soul: string) => get(env, dbi, soul),
    getJsonString: (soul: string) => getJsonString(env, dbi, soul),
    getJsonStringSync: (soul: string) => getJsonStringSync(env, dbi, soul),
    getSync: (soul: string) => getSync(env, dbi, soul),
    put: (graphData: GunGraphData) => put(env, dbi, graphData),
    putSync: (graphData: GunGraphData) => putSync(env, dbi, graphData)
  }
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
  soul: string
): GunNode | null {
  if (!soul) {
    return null
  }

  return transaction(env, txn => deserialize(txn.getStringUnsafe(dbi, soul)), {
    readOnly: true
  })
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
  soul: string
): Promise<GunNode | null> {
  return getSync(env, dbi, soul)
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
  soul: string
): string {
  if (!soul) {
    return ''
  }

  return transaction(env, txn => txn.getString(dbi, soul) || '', {
    readOnly: true
  })
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
  soul: string
): Promise<string> {
  return getJsonStringSync(env, dbi, soul)
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

  // tslint:disable-next-line: no-expression-statement
  txn.putString(dbi, soul, serialize(result!))

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
      const node = deserialize(existingData) || undefined
      const nodeDiff = putNode(dbi, txn, soul, node, graphData[soul]!, opts)

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
