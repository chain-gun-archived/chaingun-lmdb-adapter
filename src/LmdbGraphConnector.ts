import { GunGraphWireConnector } from '@notabug/chaingun'
import { DEFAULT_CONFIG, GunLmdbClient } from './GunLmdbClient'

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
    ;(async () => {
      const node = await this._client.get(soul)
      const put: GunGraphData | null = node
        ? {
            [soul]: node
          }
        : null
      const msg: GunMsg = { put }
      if (msgId) msg['@'] = msgId
      if (cb) cb(msg)
      if (put) {
        this.events.graphData.trigger(put)
      } else {
        this.events.graphData.trigger({ [soul]: undefined })
      }
    })()
    return () => {}
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
    ;(async () => {
      try {
        await this._client.write(graph)
        if (cb) {
          cb({
            '@': msgId,
            ok: true,
            err: null
          })
        }
      } catch (err) {
        if (cb) {
          cb({
            '@': msgId,
            ok: false,
            err
          })
        } else {
          console.warn(err.stack || err)
        }
      }
    })()

    return () => {}
  }
}
