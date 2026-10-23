import { afterEach, describe, expect, it, vi } from 'vitest'
import { fault, op, scenario, setSpeed } from './api'

function respond(status: number, body: unknown) {
  const fetchMock = vi.fn(async (_url: string, _init?: RequestInit) => new Response(JSON.stringify(body), { status }))
  vi.stubGlobal('fetch', fetchMock)
  return fetchMock
}

afterEach(() => vi.unstubAllGlobals())

describe('api', () => {
  it('op posts the op as JSON and resolves with the Ok payload', async () => {
    const f = respond(200, { Ok: 'tok' })
    await expect(op('CreateJoinToken')).resolves.toBe('tok')
    expect(f).toHaveBeenCalledWith('/api/ops', expect.objectContaining({ method: 'POST', body: '"CreateJoinToken"' }))
  })

  it('rejects with the server error string on 409 and 503', async () => {
    respond(409, { error: 'simulation paused' })
    await expect(op('CreateJoinToken')).rejects.toBe('simulation paused')
    respond(503, { error: 'control plane unavailable' })
    await expect(fault('HealPartitions')).rejects.toBe('control plane unavailable')
  })

  it('rejects with the server error string on a rejected op too', async () => {
    respond(400, { error: 'no such host' })
    await expect(op({ Cordon: { id: 'x' } })).rejects.toBe('no such host')
  })

  it('falls back to the status when the body has no error', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response('bad gateway', { status: 502 })))
    await expect(setSpeed(5)).rejects.toBe('HTTP 502')
  })

  it('posts speed, fault and scenario to their endpoints', async () => {
    const f = respond(200, { ok: true })
    await setSpeed(20)
    await fault({ KillHost: { host: 'h1' } })
    await scenario({ preset: 'scale500' })
    expect(f.mock.calls.map((c) => [c[0], c[1]?.body])).toEqual([
      ['/api/sim/speed', '{"speed":20}'],
      ['/api/sim/fault', '{"KillHost":{"host":"h1"}}'],
      ['/api/sim/scenario', '{"preset":"scale500"}'],
    ])
  })
})
