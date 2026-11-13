import { describe, expect, it } from 'vitest'
import { authReducer, loginError, type AuthState } from './auth'
import type { AuthStatus } from './gen/AuthStatus'

const methods: AuthStatus['methods'] = [{ kind: 'password' }]
const as = (user: string | null): AuthStatus => ({ session: user ? { user, method: { kind: 'password' }, expires_at: 1 } : null, methods })

describe('authReducer', () => {
  it('opens on the login page when nobody is logged in, and the workspace when someone is', () => {
    expect(authReducer({ phase: 'checking' }, { type: 'status', status: as(null) })).toEqual({ phase: 'anonymous', methods })
    const s = authReducer({ phase: 'checking' }, { type: 'status', status: as('alice') })
    expect(s).toMatchObject({ phase: 'in', epoch: 1, session: { user: 'alice' } })
  })

  it('keeps the workspace behind a dialog when the session ends under it, and reconnects after the next login', () => {
    const inside: AuthState = { phase: 'in', session: as('alice').session!, methods, epoch: 1 }
    const lost = authReducer(inside, { type: 'expired' })
    expect(lost).toEqual({ phase: 'lost', user: 'alice', methods, epoch: 1 })
    expect(authReducer(inside, { type: 'status', status: as(null) })).toEqual(lost)
    expect(authReducer(lost, { type: 'expired' })).toBe(lost)
    expect(authReducer(lost, { type: 'logged-in', status: as('alice') })).toMatchObject({ phase: 'in', epoch: 2 })
  })

  it('does not reconnect for a check that finds the same session', () => {
    const inside: AuthState = { phase: 'in', session: as('alice').session!, methods, epoch: 3 }
    expect(authReducer(inside, { type: 'status', status: as('alice') })).toMatchObject({ phase: 'in', epoch: 3 })
    expect(authReducer(inside, { type: 'status', status: as('bob') })).toMatchObject({ phase: 'in', epoch: 4, session: { user: 'bob' } })
  })

  it('goes back to the login page on logging out, and an expiry then changes nothing', () => {
    const inside: AuthState = { phase: 'in', session: as('alice').session!, methods, epoch: 1 }
    const out = authReducer(inside, { type: 'logged-out' })
    expect(out).toEqual({ phase: 'anonymous', methods })
    expect(authReducer(out, { type: 'expired' })).toBe(out)
  })
})

describe('an unreachable server', () => {
  it('shows on first load instead of waiting forever, and clears with the first answer', () => {
    const off = authReducer({ phase: 'checking' }, { type: 'unreachable', reason: 'cannot reach the server' })
    expect(off).toEqual({ phase: 'unreachable', reason: 'cannot reach the server' })
    expect(authReducer(off, { type: 'unreachable', reason: 'cannot reach the server' })).toBe(off)
    expect(authReducer(off, { type: 'unreachable', reason: 'the server answered HTTP 502' })).toEqual({ phase: 'unreachable', reason: 'the server answered HTTP 502' })
    expect(authReducer(off, { type: 'status', status: as(null) })).toEqual({ phase: 'anonymous', methods })
    expect(authReducer(off, { type: 'status', status: as('alice') })).toMatchObject({ phase: 'in', epoch: 1 })
  })

  it('leaves a running workspace, the login page and the login dialog as they are', () => {
    const down = { type: 'unreachable', reason: 'cannot reach the server' } as const
    const inside: AuthState = { phase: 'in', session: as('alice').session!, methods, epoch: 1 }
    const lost: AuthState = { phase: 'lost', user: 'alice', methods, epoch: 1 }
    const anonymous: AuthState = { phase: 'anonymous', methods }
    for (const s of [inside, lost, anonymous]) expect(authReducer(s, down)).toBe(s)
  })
})

describe('loginError', () => {
  it("prefers the server's words", () => {
    expect(loginError(429, { error: 'too many failed logins: try again in 4 s' })).toBe('too many failed logins: try again in 4 s')
    expect(loginError(401, null)).toBe('wrong user name or password')
    expect(loginError(429, null)).toMatch(/too many/)
    expect(loginError(502, 'gateway')).toBe('the server answered HTTP 502')
  })
})
