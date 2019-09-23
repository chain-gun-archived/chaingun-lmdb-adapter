import { GunGraphWireConnector, generateMessageId } from '@notabug/chaingun'
import { DEFAULT_CONFIG, GunLmdbClient } from './GunLmdbClient'

const NOOP = () => {}

export class LmdbGraphConnector extends GunGraphWireConnector {
  protected _client: GunLmdbClient

  constructor(lmdbOpts = DEFAULT_CONFIG) {
    super('LmdbGraphConnector')
    this._client = new GunLmdbClient(lmdbOpts)
  }

  get({
    soul,
    cb,
    msgId = '',
    key = '' // TODO
  }: {
    soul: string
    key?: string
    msgId?: string
    cb?: GunMsgCb
  }) {
    const now = new Date().getTime()
    const node = this._client.get(soul)
    const put: GunGraphData | null = node
      ? {
          [soul]: node
        }
      : null
    const msg: GunMsg = { '#': generateMessageId(), put }
    if (msgId) msg['@'] = msgId
    if (cb) cb(msg)

    const done = new Date().getTime()
    console.log('get', soul, done - now)
    return NOOP
  }

  put({
    graph,
    msgId = '',
    cb
  }: {
    graph: GunPut
    msgId?: string
    replyTo?: string
    cb?: GunMsgCb
  }) {
    const start = new Date().getTime()
    const id = generateMessageId()
    try {
      this._client.write(graph)
      if (cb) {
        cb({
          '#': id,
          '@': msgId,
          ok: true,
          err: null
        })
      }
    } catch (err) {
      if (cb) {
        cb({
          '#': id,
          '@': msgId,
          ok: false,
          err
        })
      } else {
        console.warn(err.stack || err)
      }
    }
    const done = new Date().getTime()
    console.log('write', done - start, Object.keys(graph))

    return NOOP
  }
}
