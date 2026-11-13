import { useSyncExternalStore } from 'react'
import type { AuthMethod } from './gen/AuthMethod'
import type { AuthSession } from './gen/AuthSession'
import type { AuthStatus } from './gen/AuthStatus'

/** The WebSocket close code the server sends when the session behind a socket ends. */
export const SESSION_ENDED = 4401

/**
 * Where the login stands:
 * - `checking`: asking the server on first load;
 * - `unreachable`: that first question got no answer (server down or
 *   restarting, a proxy error): say so and keep asking, rather than wait forever;
 * - `anonymous`: nobody logged in (first visit, or after signing out): the login page;
 * - `in`: logged in; `epoch` counts logins, so a new one reconnects the live view;
 * - `lost`: the session ended under a running UI (expired, password changed,
 *   server restarted): the workspace stays on screen, behind a login dialog,
 *   so nothing half-edited is thrown away.
 */
export type AuthState =
  | { phase: 'checking' }
  | { phase: 'unreachable'; reason: string }
  | { phase: 'anonymous'; methods: AuthMethod[] }
  | { phase: 'in'; session: AuthSession; methods: AuthMethod[]; epoch: number }
  | { phase: 'lost'; user: string; methods: AuthMethod[]; epoch: number }

export type AuthEvent =
  | { type: 'status'; status: AuthStatus }
  | { type: 'logged-in'; status: AuthStatus }
  | { type: 'expired' }
  | { type: 'logged-out' }
  | { type: 'unreachable'; reason: string }

const DEFAULT_METHODS: AuthMethod[] = [{ kind: 'password' }]

function methodsOf(s: AuthState): AuthMethod[] {
  return s.phase === 'checking' || s.phase === 'unreachable' ? DEFAULT_METHODS : s.methods
}

function epochOf(s: AuthState): number {
  return s.phase === 'in' || s.phase === 'lost' ? s.epoch : 0
}

export function authReducer(s: AuthState, e: AuthEvent): AuthState {
  switch (e.type) {
    case 'status':
    case 'logged-in': {
      const { session, methods } = e.status
      if (session) {
        // A status check that finds the same session changes nothing (no reconnect).
        if (e.type === 'status' && s.phase === 'in' && s.session.user === session.user) return { ...s, session, methods }
        return { phase: 'in', session, methods, epoch: epochOf(s) + 1 }
      }
      if (s.phase === 'in') return { phase: 'lost', user: s.session.user, methods, epoch: s.epoch }
      if (s.phase === 'lost') return { ...s, methods }
      return { phase: 'anonymous', methods }
    }
    case 'expired':
      return s.phase === 'in' ? { phase: 'lost', user: s.session.user, methods: s.methods, epoch: s.epoch } : s
    case 'logged-out':
      return { phase: 'anonymous', methods: methodsOf(s) }
    case 'unreachable':
      // Only before we know who is logged in: a running workspace keeps
      // going, and its live view shows that the server is away.
      if (s.phase === 'checking') return { phase: 'unreachable', reason: e.reason }
      if (s.phase === 'unreachable' && s.reason !== e.reason) return { ...s, reason: e.reason }
      return s
  }
}

/** What a failed login says, from the server's answer. */
export function loginError(status: number, body: unknown): string {
  const error = (body as { error?: unknown } | null)?.error
  if (typeof error === 'string') return error
  if (status === 401) return 'wrong user name or password'
  if (status === 429) return 'too many failed logins: wait a little and try again'
  return `the server answered HTTP ${status}`
}

type Listener = () => void

class AuthStore {
  private state: AuthState = { phase: 'checking' }
  private listeners = new Set<Listener>()

  getState = (): AuthState => this.state

  subscribe = (fn: Listener): (() => void) => {
    this.listeners.add(fn)
    return () => this.listeners.delete(fn)
  }

  dispatch(e: AuthEvent) {
    const next = authReducer(this.state, e)
    if (next === this.state) return
    this.state = next
    for (const fn of this.listeners) fn()
  }

  /**
   * Asks the server who we are. A server that cannot be reached leaves a
   * running workspace as it is; on first load it shows as `unreachable`.
   */
  async refresh(): Promise<AuthStatus | null> {
    let res: Response
    try {
      res = await fetch('/api/auth/session')
    } catch {
      this.dispatch({ type: 'unreachable', reason: 'cannot reach the server' })
      return null
    }
    const status = res.ok ? ((await res.json().catch(() => null)) as AuthStatus | null) : null
    if (status === null) {
      this.dispatch({ type: 'unreachable', reason: `the server answered HTTP ${res.status}` })
      return null
    }
    this.dispatch({ type: 'status', status })
    return status
  }

  /** Logs in; rejects with what to tell the user. */
  async login(username: string, password: string): Promise<void> {
    let res: Response
    try {
      res = await fetch('/api/auth/login', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ username, password }),
      })
    } catch {
      throw 'cannot reach the server'
    }
    const body: unknown = await res.json().catch(() => null)
    if (!res.ok) throw loginError(res.status, body)
    this.dispatch({ type: 'logged-in', status: body as AuthStatus })
  }

  async logout(): Promise<void> {
    await fetch('/api/auth/logout', { method: 'POST' }).catch(() => undefined)
    this.dispatch({ type: 'logged-out' })
  }

  /** A request was refused for want of a session. */
  expired() {
    this.dispatch({ type: 'expired' })
  }
}

export const auth = new AuthStore()

export function useAuth(): AuthState {
  return useSyncExternalStore(auth.subscribe, auth.getState)
}

/** `fetch` for the API: a 401 means the session has ended, and the login dialog opens. */
export async function apiFetch(input: string, init?: RequestInit): Promise<Response> {
  const res = await (init === undefined ? fetch(input) : fetch(input, init))
  if (res.status === 401) auth.expired()
  return res
}
