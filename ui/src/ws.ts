import { SESSION_ENDED } from './auth'
import type { ServerMsg } from './gen/ServerMsg'
import type { Store } from './store'

const RECONNECT_MS = 1000

/** The WebSocket URL for `/api/ws` on the page's own host (the Vite dev proxy forwards it in development). */
export function wsUrl(loc: Pick<Location, 'protocol' | 'host'> = window.location): string {
  return `${loc.protocol === 'https:' ? 'wss' : 'ws'}://${loc.host}/api/ws`
}

export interface ConnectOptions {
  /** The session behind the socket ended: stop, and let the login take over. */
  onSessionEnded?: () => void
  /**
   * Asked after a drop that did not say why: whether we are still logged in
   * (a refused upgrade reaches the browser as a bare close). Undefined (the
   * server cannot be reached) keeps reconnecting.
   */
  stillLoggedIn?: () => Promise<boolean | undefined>
}

/**
 * Feeds `store` from the server's WebSocket and reconnects after a drop.
 * Each connection starts with a snapshot, so nothing needs replaying.
 * Returns a function that closes the connection for good.
 */
export function connect(store: Store, url: string = wsUrl(), opts: ConnectOptions = {}): () => void {
  let socket: WebSocket | null = null
  let timer: ReturnType<typeof setTimeout> | undefined
  let closed = false

  const open = () => {
    const ws = new WebSocket(url)
    socket = ws
    ws.onmessage = (e) => store.applyMessage(JSON.parse(e.data as string) as ServerMsg)
    ws.onclose = (e) => {
      if (closed || socket !== ws) return
      store.setDisconnected()
      const ended = () => {
        closed = true
        opts.onSessionEnded?.()
      }
      if (e.code === SESSION_ENDED) return ended()
      timer = setTimeout(async () => {
        if (closed) return
        if (opts.stillLoggedIn && (await opts.stillLoggedIn()) === false) return ended()
        if (!closed) open()
      }, RECONNECT_MS)
    }
  }
  open()

  return () => {
    closed = true
    clearTimeout(timer)
    socket?.close()
  }
}
