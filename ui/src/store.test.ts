import { describe, expect, it, vi } from 'vitest'
import type { Alert } from './gen/Alert'
import type { Event as ClusterEvent } from './gen/Event'
import type { GroundTruth } from './gen/GroundTruth'
import type { HostObj } from './gen/HostObj'
import type { ServerMsg } from './gen/ServerMsg'
import type { SimInfo } from './gen/SimInfo'
import type { ViewSnapshot } from './gen/ViewSnapshot'
import { EVENT_CAP, STALE_AFTER_MS, createStore } from './store'

const truth: GroundTruth = { hosts: [], vip_owner_by_switch: {}, partitions: [], split_brain_vms: [], ports: [], flows: [], sim_time: 0 }

function host(id: string, lifecycle: HostObj['lifecycle'] = 'Ready'): HostObj {
  return {
    id,
    lifecycle,
    roles: [],
    labels: {},
    location: 'rack-1',
    before_not_ready: null,
    last_transition: 0,
    version: 1,
  } as unknown as HostObj
}

function event(seq: number): ClusterEvent {
  return { seq, at: seq, entity: { kind: 'Host', id: 'a' }, message: `e${seq}`, version: seq }
}

function snapshot(over: Partial<ViewSnapshot> = {}, sim: Partial<SimInfo> = {}): ServerMsg {
  const view: ViewSnapshot = {
    version: 1,
    leader: 'cp-1',
    term: 1,
    last_sync_at: 1000,
    hosts: [host('a'), host('b')],
    tokens: [],
    rules: [],
    switches: [],
    networks: [],
    templates: [],
    rollouts: [],
    alerts: [],
    events: [],
    cp: { voters: ['cp-1'], mode: 'Suggest', version: 1 },
    metrics: {},
    ...over,
  }
  return { type: 'snapshot', view, truth, sim: { now: 2000, speed: 1, paused: false, ...sim } }
}

function delta(over: Partial<Extract<ServerMsg, { type: 'delta' }>> = {}): ServerMsg {
  return {
    type: 'delta',
    version: 2,
    leader: 'cp-1',
    term: 1,
    last_sync_at: 1000,
    upserts: [],
    removed: [],
    metrics: {},
    truth,
    sim: { now: 2500, speed: 1, paused: false },
    ...over,
  }
}

describe('store', () => {
  it('starts connecting with an empty view', () => {
    const s = createStore()
    expect(s.getState().connection).toBe('connecting')
    expect(s.getState().view.hosts).toEqual({})
  })

  it('snapshot then delta merges upserts and removes', () => {
    const s = createStore()
    s.applyMessage(snapshot())
    expect(Object.keys(s.getState().view.hosts).sort()).toEqual(['a', 'b'])

    s.applyMessage(
      delta({
        upserts: [{ type: 'Host', ...host('a', 'NotReady') }],
        removed: [{ kind: 'Host', id: 'b' }],
      }),
    )
    const hosts = s.getState().view.hosts
    expect(Object.keys(hosts)).toEqual(['a'])
    expect(hosts.a.lifecycle).toBe('NotReady')
    expect(s.getState().view.version).toBe(2)
  })

  it('a snapshot replaces everything', () => {
    const s = createStore()
    s.applyMessage(snapshot({ events: [event(1)] }))
    s.applyMessage(snapshot({ hosts: [host('x')], events: [event(7)] }))
    expect(Object.keys(s.getState().view.hosts)).toEqual(['x'])
    expect(s.getState().view.events.map((e) => e.seq)).toEqual([7])
  })

  it('merges alerts, events, tokens, rules and the control plane', () => {
    const s = createStore()
    s.applyMessage(snapshot())
    const alert: Alert = {
      id: 'al1',
      severity: 'Warning',
      kind: 'HostDown',
      entity: { kind: 'Host', id: 'a' },
      message: 'down',
      raised_at: 5,
      resolved_at: null,
      ground_truth: false,
      version: 2,
    }
    s.applyMessage(
      delta({
        upserts: [
          { type: 'Alert', ...alert },
          { type: 'Event', ...event(3) },
          { type: 'Token', token: 't', expires_at: 9, version: 2 },
          { type: 'Rule', id: 'r', profile: 'Pi5', roles: ['Compute'], template: null, version: 2 },
          { type: 'Cp', voters: ['cp-1', 'cp-2'], mode: 'Auto', version: 2 },
        ],
      }),
    )
    const v = s.getState().view
    expect(v.alerts.al1.message).toBe('down')
    expect(v.events.map((e) => e.seq)).toEqual([3])
    expect(v.tokens.t.expires_at).toBe(9)
    expect(v.rules.r.profile).toBe('Pi5')
    expect(v.cp).toEqual({ voters: ['cp-1', 'cp-2'], mode: 'Auto', version: 2 })

    s.applyMessage(delta({ removed: [{ kind: 'Alert', id: 'al1' }, { kind: 'Token', id: 't' }, { kind: 'Rule', id: 'r' }] }))
    const after = s.getState().view
    expect([after.alerts, after.tokens, after.rules].map((r) => Object.keys(r).length)).toEqual([0, 0, 0])
  })

  it('stores observed switches from snapshots and deltas', () => {
    const sw = (last_seen: number) => ({ id: 'access-1', model: 'm', ports: [], mlag_peer: null, last_seen, version: last_seen })
    const s = createStore()
    s.applyMessage(snapshot({ switches: [sw(1)] }))
    expect(s.getState().view.switches['access-1'].last_seen).toBe(1)

    s.applyMessage(delta({ upserts: [{ type: 'Switch', ...sw(61_000) }] }))
    expect(s.getState().view.switches['access-1'].last_seen).toBe(61_000)
    expect(s.getState().view.switches['access-1']).not.toHaveProperty('type')

    s.applyMessage(delta({ removed: [{ kind: 'Switch', id: 'access-1' }] }))
    expect(s.getState().view.switches).toEqual({})
    s.applyMessage(snapshot())
    expect(s.getState().view.switches).toEqual({})
  })

  it('stores networks, templates and rollouts from snapshots and deltas', () => {
    const net = (mtu: number) => ({ id: 'storage', purpose: 'Storage', vlan: 20, subnet: '10.20.0.0/22', mtu, version: mtu })
    const tpl = { id: 'ms01-standard', profile: 'Ms01', role: null, config: { links: [], attachments: [] }, version: 1 }
    const rollout = { id: 'r1', state: 'Running', hosts: ['a'], version: 1 }
    const s = createStore()
    s.applyMessage(snapshot({ networks: [net(1500)] as never }))
    expect(s.getState().view.networks.storage.mtu).toBe(1500)

    s.applyMessage(
      delta({
        upserts: [
          { type: 'Network', ...net(9000) },
          { type: 'Template', ...tpl },
          { type: 'Rollout', ...rollout },
        ] as never,
      }),
    )
    const v = s.getState().view
    expect(v.networks.storage.mtu).toBe(9000)
    expect(v.networks.storage).not.toHaveProperty('type')
    expect(v.templates['ms01-standard'].profile).toBe('Ms01')
    expect(v.templates['ms01-standard']).not.toHaveProperty('type')
    expect(v.rollouts.r1.hosts).toEqual(['a'])
    expect(v.rollouts.r1).not.toHaveProperty('type')

    s.applyMessage(
      delta({
        removed: [
          { kind: 'Network', id: 'storage' },
          { kind: 'Template', id: 'ms01-standard' },
          { kind: 'Rollout', id: 'r1' },
        ],
      }),
    )
    const after = s.getState().view
    expect([after.networks, after.templates, after.rollouts].map((r) => Object.keys(r).length)).toEqual([0, 0, 0])
    s.applyMessage(snapshot())
    expect(s.getState().view.rollouts).toEqual({})
  })

  it('keeps events ordered by seq and capped at the newest EVENT_CAP', () => {
    const s = createStore()
    s.applyMessage(snapshot())
    const upserts = []
    for (let seq = EVENT_CAP + 10; seq >= 1; seq--) upserts.push({ type: 'Event' as const, ...event(seq) })
    s.applyMessage(delta({ upserts }))
    const seqs = s.getState().view.events.map((e) => e.seq)
    expect(seqs).toHaveLength(EVENT_CAP)
    expect(seqs[0]).toBe(11)
    expect(seqs[seqs.length - 1]).toBe(EVENT_CAP + 10)
  })

  it('delta metrics merge over the latest samples and a removed host drops its sample', () => {
    const s = createStore()
    const sample = (at: number) => ({
      at, cpu_pct: 1, ram_pct: 1, temp_c: 1, disk_pct: {}, port_rx_bps: {}, port_tx_bps: {}, instances: 0,
    })
    s.applyMessage(snapshot({ metrics: { a: sample(1), b: sample(1) } }))
    s.applyMessage(delta({ metrics: { a: sample(2) } }))
    expect(s.getState().view.metrics.a.at).toBe(2)
    expect(s.getState().view.metrics.b.at).toBe(1)
    s.applyMessage(delta({ removed: [{ kind: 'Host', id: 'b' }] }))
    expect(Object.keys(s.getState().view.metrics)).toEqual(['a'])
  })

  it('takes leader, term, truth and sim from every delta', () => {
    const s = createStore()
    s.applyMessage(snapshot())
    s.applyMessage(delta({ leader: 'cp-2', term: 4, sim: { now: 9000, speed: 20, paused: false } }))
    expect(s.getState().view.leader).toBe('cp-2')
    expect(s.getState().view.term).toBe(4)
    expect(s.getState().sim).toEqual({ now: 9000, speed: 20, paused: false })
  })

  it('stale sync flips connection to cp-unavailable', () => {
    const s = createStore()
    s.applyMessage(snapshot({ last_sync_at: 1000 }, { now: 1000 + STALE_AFTER_MS }))
    expect(s.getState().connection).toBe('live')

    s.applyMessage(delta({ last_sync_at: 1000, sim: { now: 1000 + STALE_AFTER_MS + 1, speed: 1, paused: false } }))
    expect(s.getState().connection).toBe('cp-unavailable')

    s.applyMessage(delta({ last_sync_at: 5000, sim: { now: 5100, speed: 1, paused: false } }))
    expect(s.getState().connection).toBe('live')
  })

  it('a client that never synced is cp-unavailable', () => {
    const s = createStore()
    s.applyMessage(snapshot({ last_sync_at: null }))
    expect(s.getState().connection).toBe('cp-unavailable')
  })

  it('a dropped socket is connecting again, keeping the last view', () => {
    const s = createStore()
    s.applyMessage(snapshot())
    s.setDisconnected()
    expect(s.getState().connection).toBe('connecting')
    expect(Object.keys(s.getState().view.hosts)).toHaveLength(2)
  })

  it('notifies subscribers until they unsubscribe', () => {
    const s = createStore()
    const fn = vi.fn()
    const off = s.subscribe(fn)
    s.applyMessage(snapshot())
    expect(fn).toHaveBeenCalledTimes(1)
    off()
    s.applyMessage(delta())
    expect(fn).toHaveBeenCalledTimes(1)
  })

  it('keeps unchanged slices referentially equal across deltas', () => {
    const s = createStore()
    s.applyMessage(snapshot())
    const before = s.getState().view
    s.applyMessage(delta())
    const after = s.getState().view
    expect(after.hosts).toBe(before.hosts)
    expect(after.alerts).toBe(before.alerts)
    expect(after.events).toBe(before.events)
  })
})
