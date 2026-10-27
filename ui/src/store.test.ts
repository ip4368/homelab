import { describe, expect, it, vi } from 'vitest'
import type { Alert } from './gen/Alert'
import type { Event as ClusterEvent } from './gen/Event'
import type { HostObj } from './gen/HostObj'
import type { ServerMsg } from './gen/ServerMsg'
import type { ClockInfo } from './gen/ClockInfo'
import type { LabTruth } from './gen/LabTruth'
import type { ViewSnapshot } from './gen/ViewSnapshot'
import { EVENT_CAP, STALE_AFTER_MS, createStore } from './store'

const truth: LabTruth = { vms: [], switches: [], ports: [], partitions: [], vip_holders: [], split_brain_vms: [], at: 0 }

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

function snapshot(over: Partial<ViewSnapshot> = {}, clock: Partial<ClockInfo> = {}): ServerMsg {
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
    deployments: [],
    pods: [],
    vms: [],
    vm_status: [],
    volumes: [],
    volume_status: [],
    alerts: [],
    events: [],
    cp: { voters: ['cp-1'], mode: 'Suggest', version: 1 },
    metrics: {},
    k8s: null,
    ...over,
  }
  return { type: 'snapshot', view, truth, clock: { now: 2000, ...clock } }
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
    k8s: null,
    truth,
    clock: { now: 2500 },
    ...over,
  }
}

describe('store', () => {
  it('starts connecting with an empty view', () => {
    const s = createStore()
    expect(s.getState().connection).toBe('connecting')
    expect(s.getState().view.hosts).toEqual({})
  })

  it('carries the observed snapshot, replacing it only on a newer take', () => {
    const at = (n: number) => ({ at: n, nodes: [], deployments: [], pods: [], lh_nodes: [], volumes: [] })
    const s = createStore()
    s.applyMessage(snapshot({ k8s: at(5) }))
    expect(s.getState().view.k8s?.at).toBe(5)
    s.applyMessage(delta())
    expect(s.getState().view.k8s?.at).toBe(5)
    s.applyMessage(delta({ k8s: at(35) }))
    expect(s.getState().view.k8s?.at).toBe(35)
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

  it('stores workloads from snapshots and deltas, pods keyed "<deployment>/<idx>"', () => {
    const dep = (replicas: number) => ({ version: replicas, id: 'web', image: 'nginx', replicas, cpu_m: 100, ram_mb: 64, selector: { roles: [], capability_prefix: null } })
    const pod = (idx: number, host: string | null) => ({ version: 1, deployment: 'web', idx, host, phase: 'Running' })
    const vm = { version: 1, id: 'vm1', vcpu: 2, ram_mb: 2048, disk_gb: 20, networks: ['vm-lan'], volume: 'vol1' }
    const vmStatus = (host: string | null) => ({ id: 'vm1', version: 2, host, phase: 'Running', since: 5 })
    const vol = { version: 1, id: 'vol1', size_gb: 20, replicas: 3 }
    const volStatus = (health: string) => ({ id: 'vol1', version: 2, replicas: [], health })
    const s = createStore()
    s.applyMessage(snapshot({ deployments: [dep(2)], pods: [pod(0, 'a'), pod(1, null)], vms: [vm], vm_status: [vmStatus('a')], volumes: [vol], volume_status: [volStatus('Degraded')] } as never))
    let v = s.getState().view
    expect(v.deployments.web.replicas).toBe(2)
    expect(Object.keys(v.pods).sort()).toEqual(['web/0', 'web/1'])
    expect(v.vms.vm1.volume).toBe('vol1')
    expect(v.vmStatus.vm1.host).toBe('a')
    expect(v.volumes.vol1.size_gb).toBe(20)
    expect(v.volumeStatus.vol1.health).toBe('Degraded')

    s.applyMessage(
      delta({
        upserts: [
          { type: 'Deployment', ...dep(3) },
          { type: 'Pod', ...pod(1, 'b') },
          { type: 'Pod', ...pod(2, 'a') },
          { type: 'VmStatus', ...vmStatus('b') },
          { type: 'VolumeStatus', ...volStatus('Healthy') },
        ] as never,
      }),
    )
    v = s.getState().view
    expect(v.deployments.web.replicas).toBe(3)
    expect(v.deployments.web).not.toHaveProperty('type')
    expect(v.pods['web/1'].host).toBe('b')
    expect(v.pods['web/2'].host).toBe('a')
    expect(v.pods['web/1']).not.toHaveProperty('type')
    expect(v.vmStatus.vm1.host).toBe('b')
    expect(v.volumeStatus.vol1.health).toBe('Healthy')

    s.applyMessage(
      delta({
        removed: [
          { kind: 'Pod', id: 'web/2' },
          { kind: 'Vm', id: 'vm1' },
          { kind: 'VmStatus', id: 'vm1' },
          { kind: 'Volume', id: 'vol1' },
          { kind: 'VolumeStatus', id: 'vol1' },
          { kind: 'Deployment', id: 'web' },
        ],
      }),
    )
    const after = s.getState().view
    expect(Object.keys(after.pods).sort()).toEqual(['web/0', 'web/1'])
    expect([after.deployments, after.vms, after.vmStatus, after.volumes, after.volumeStatus].map((r) => Object.keys(r).length)).toEqual([0, 0, 0, 0, 0])
    s.applyMessage(snapshot())
    expect(s.getState().view.pods).toEqual({})
  })

  it('a delta without workload changes keeps the workload slices', () => {
    const s = createStore()
    s.applyMessage(snapshot())
    const before = s.getState().view
    s.applyMessage(delta())
    const after = s.getState().view
    expect([after.deployments, after.pods, after.vms, after.vmStatus, after.volumes, after.volumeStatus]).toEqual(
      [before.deployments, before.pods, before.vms, before.vmStatus, before.volumes, before.volumeStatus],
    )
    expect(after.pods).toBe(before.pods)
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

  it('takes leader, term, truth and clock from every delta', () => {
    const s = createStore()
    s.applyMessage(snapshot())
    s.applyMessage(delta({ leader: 'cp-2', term: 4, clock: { now: 9000 } }))
    expect(s.getState().view.leader).toBe('cp-2')
    expect(s.getState().view.term).toBe(4)
    expect(s.getState().clock).toEqual({ now: 9000 })
  })

  it('stale sync flips connection to cp-unavailable', () => {
    const s = createStore()
    s.applyMessage(snapshot({ last_sync_at: 1000 }, { now: 1000 + STALE_AFTER_MS }))
    expect(s.getState().connection).toBe('live')

    s.applyMessage(delta({ last_sync_at: 1000, clock: { now: 1000 + STALE_AFTER_MS + 1 } }))
    expect(s.getState().connection).toBe('cp-unavailable')

    s.applyMessage(delta({ last_sync_at: 5000, clock: { now: 5100 } }))
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
