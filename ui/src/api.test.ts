import { afterEach, describe, expect, it, vi } from 'vitest'
import { kubeLogs, labConsole, labFault, labJournal, op } from './api'

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

  it('rejects with the server error string on 503', async () => {
    respond(503, { error: 'control plane unavailable' })
    await expect(op('CreateJoinToken')).rejects.toBe('control plane unavailable')
    respond(503, { error: 'no lab attached' })
    await expect(labFault('Heal')).rejects.toBe('no lab attached')
  })

  it('rejects with the server error string on a rejected op too', async () => {
    respond(400, { error: 'no such host' })
    await expect(op({ Cordon: { id: 'x' } })).rejects.toBe('no such host')
  })

  it('falls back to the status when the body has no error', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response('bad gateway', { status: 502 })))
    await expect(labFault('Heal')).rejects.toBe('HTTP 502')
  })

  it('posts lab faults to their endpoint and returns a racked machine', async () => {
    const f = respond(200, { ok: true })
    await expect(labFault({ Freeze: { vm: 'ms01-01' } })).resolves.toBeNull()
    expect(f.mock.calls.map((c) => [c[0], c[1]?.body])).toEqual([['/api/lab/fault', '{"Freeze":{"vm":"ms01-01"}}']])
    respond(200, { ok: true, vm: 'ms01-04' })
    await expect(labFault({ AddVm: { profile: 'Ms01', mistake: null } })).resolves.toBe('ms01-04')
  })

  it('gets log and journal text from their endpoints, with their errors', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response('line one\n', { status: 200 })))
    await expect(kubeLogs({ namespace: 'demo', pod: 'web-1', container: 'app', tail: 50 })).resolves.toBe('line one\n')
    const f = respond(200, {})
    await kubeLogs({ namespace: 'demo', pod: 'web-1', previous: true })
    expect(f.mock.calls.map((c) => c[0])).toEqual(['/api/kube/logs?namespace=demo&pod=web-1&previous=true'])
    // Re-stub as text endpoints do: a 400 carries the server's words.
    respond(400, { error: '".." is not a Kubernetes namespace and pod' })
    await expect(kubeLogs({ namespace: 'demo', pod: '..' })).rejects.toContain('not a Kubernetes')
    respond(503, { error: 'no lab attached' })
    await expect(labConsole('ms01-01')).rejects.toBe('no lab attached')
    await expect(labJournal('ms01-01', 'k3s', 10)).rejects.toBe('no lab attached')
  })
})
