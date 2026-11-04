/**
 * The browser side of the server's terminal protocol (`/api/kube/exec`, and
 * a VM's console later): binary frames are terminal bytes both ways; JSON in
 * text frames says the rest.
 */

/** What to open a shell in. */
export interface ExecTarget {
  namespace: string
  pod: string
  container: string | null
}

/** The WebSocket URL of a shell in `t`, on the server the page came from. */
export function execUrl(t: ExecTarget, page: { protocol: string; host: string }): string {
  const q = new URLSearchParams({ namespace: t.namespace, pod: t.pod })
  if (t.container) q.set('container', t.container)
  return `${page.protocol === 'https:' ? 'wss' : 'ws'}://${page.host}/api/kube/exec?${q}`
}

/** A VM, for its serial console. */
export interface ConsoleTarget {
  namespace: string
  name: string
}

/** The WebSocket URL of `t`'s serial console. */
export function consoleUrl(t: ConsoleTarget, page: { protocol: string; host: string }): string {
  const q = new URLSearchParams({ namespace: t.namespace, name: t.name })
  return `${page.protocol === 'https:' ? 'wss' : 'ws'}://${page.host}/api/kube/console?${q}`
}

export function resizeMessage(cols: number, rows: number): string {
  return JSON.stringify({ resize: { cols, rows } })
}

/** How a session ended, from the server's last text frame. */
export type Ending = { exit: number } | { error: string }

export function parseEnding(text: string): Ending | null {
  try {
    const v = JSON.parse(text) as { exit?: { code?: unknown }; error?: unknown }
    if (typeof v.exit?.code === 'number') return { exit: v.exit.code }
    if (typeof v.error === 'string') return { error: v.error }
  } catch {
    // Not ours.
  }
  return null
}

/** The line the terminal prints when the session ends. */
export function endingLine(e: Ending | null): string {
  if (e === null) return 'connection closed'
  if ('error' in e) return e.error
  return e.exit === 0 ? 'session ended' : `session ended with exit code ${e.exit}`
}
