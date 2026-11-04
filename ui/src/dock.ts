/**
 * The dock at the bottom of the page: one tab per log or terminal session.
 * It outlives the drawers, so a log can be watched while acting elsewhere.
 */
import { useSyncExternalStore } from 'react'
import type { KubePod } from './gen/KubePod'
import type { LogTarget } from './logs'
import type { ExecTarget } from './terminal'

export type Session = { id: string; kind: 'logs'; title: string; target: LogTarget } | { id: string; kind: 'terminal'; title: string; target: ExecTarget }

export interface DockState {
  sessions: Session[]
  /** The tab shown; null when the dock is closed. */
  active: string | null
}

export const emptyDock: DockState = { sessions: [], active: null }

/** A session's identity: opening the same thing again shows its tab rather than a second one. */
export function sessionId(kind: Session['kind'], t: LogTarget): string {
  return `${kind}:${t.namespace}/${t.pod}/${t.container ?? ''}${t.previous ? ':previous' : ''}`
}

let terminals = 0

export function openSession(s: DockState, session: Session): DockState {
  if (s.sessions.some((x) => x.id === session.id)) return { ...s, active: session.id }
  return { sessions: [...s.sessions, session], active: session.id }
}

/** Closes a tab; the one before it (or after, for the first) is shown instead. */
export function closeSession(s: DockState, id: string): DockState {
  const at = s.sessions.findIndex((x) => x.id === id)
  if (at < 0) return s
  const sessions = s.sessions.filter((x) => x.id !== id)
  const active = s.active !== id ? s.active : (sessions[Math.max(0, at - 1)]?.id ?? null)
  return { sessions, active }
}

/** The container a pod's log opens on: its first app container (a pod with several must name one). */
export function defaultContainer(p: Pick<KubePod, 'container_states'>): string | null {
  return p.container_states.find((c) => c.role !== 'Init')?.name ?? p.container_states[0]?.name ?? null
}

export function logsSession(p: Pick<KubePod, 'namespace' | 'name' | 'container_states'>, container = defaultContainer(p), previous = false): Session {
  const target: LogTarget = { namespace: p.namespace, pod: p.name, container, previous }
  const title = `${p.name}${container && p.container_states.length > 1 ? `/${container}` : ''}${previous ? ' (previous)' : ''}`
  return { id: sessionId('logs', target), kind: 'logs', title, target }
}

/** A shell in `p`'s container: each opens its own tab, as two shells are two sessions. */
export function terminalSession(p: Pick<KubePod, 'namespace' | 'name' | 'container_states'>, container = defaultContainer(p)): Session {
  terminals += 1
  const title = `${p.name}${container && p.container_states.length > 1 ? `/${container}` : ''}`
  return { id: `terminal:${p.namespace}/${p.name}/${container ?? ''}#${terminals}`, kind: 'terminal', title, target: { namespace: p.namespace, pod: p.name, container } }
}

let state = emptyDock
const listeners = new Set<() => void>()

function set(next: DockState) {
  state = next
  listeners.forEach((l) => l())
}

export const dock = {
  open: (session: Session) => set(openSession(state, session)),
  close: (id: string) => set(closeSession(state, id)),
  show: (id: string | null) => set({ ...state, active: id }),
}

export function useDock(): DockState {
  return useSyncExternalStore(
    (l) => {
      listeners.add(l)
      return () => listeners.delete(l)
    },
    () => state,
  )
}
