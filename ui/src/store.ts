import { useSyncExternalStore } from 'react'
import type { Alert } from './gen/Alert'
import type { AutoApproveRule } from './gen/AutoApproveRule'
import type { ClockInfo } from './gen/ClockInfo'
import type { CpInfo } from './gen/CpInfo'
import type { DeploymentObj } from './gen/DeploymentObj'
import type { Event as ClusterEvent } from './gen/Event'
import type { HostObj } from './gen/HostObj'
import type { JoinToken } from './gen/JoinToken'
import type { KubeView } from './gen/KubeView'
import type { LabTruth } from './gen/LabTruth'
import type { MetricSample } from './gen/MetricSample'
import type { NetworkObj } from './gen/NetworkObj'
import type { Object as ClusterObject } from './gen/Object'
import type { ObjectRef } from './gen/ObjectRef'
import type { PodObj } from './gen/PodObj'
import type { Rollout } from './gen/Rollout'
import type { ServerMsg } from './gen/ServerMsg'
import type { SwitchObj } from './gen/SwitchObj'
import type { TemplateObj } from './gen/TemplateObj'
import type { VmObj } from './gen/VmObj'
import type { VmStatusObj } from './gen/VmStatusObj'
import type { VolumeObj } from './gen/VolumeObj'
import type { VolumeStatusObj } from './gen/VolumeStatusObj'

/** The server's wire message, as sent over `/api/ws`. */
export type WsMessage = ServerMsg

/** The UI keeps this many of the newest events; the server's ring drops old ones without tombstones. */
export const EVENT_CAP = 500
/** The control plane is unavailable when the client last synced with a leader this long (ms) ago. */
export const STALE_AFTER_MS = 3000

export type Connection = 'connecting' | 'live' | 'cp-unavailable'

export interface View {
  version: number
  leader: string | null
  term: number
  last_sync_at: number | null
  hosts: Record<string, HostObj>
  tokens: Record<string, JoinToken>
  rules: Record<string, AutoApproveRule>
  /** As observed by the control plane polling them. */
  switches: Record<string, SwitchObj>
  networks: Record<string, NetworkObj>
  templates: Record<string, TemplateObj>
  /** Every rollout, finished ones included. */
  rollouts: Record<string, Rollout>
  alerts: Record<string, Alert>
  /** Oldest first, at most EVENT_CAP. */
  events: ClusterEvent[]
  cp: CpInfo
  /** Latest sample per host. */
  metrics: Record<string, MetricSample>
  deployments: Record<string, DeploymentObj>
  /** Keyed `<deployment>/<idx>`. */
  pods: Record<string, PodObj>
  vms: Record<string, VmObj>
  /** Keyed by VM id. */
  vmStatus: Record<string, VmStatusObj>
  volumes: Record<string, VolumeObj>
  /** Keyed by volume id. */
  volumeStatus: Record<string, VolumeStatusObj>
}

export interface UiState {
  view: View
  /** What the lab sees from outside the machines. */
  truth: LabTruth
  /** What Kubernetes runs, as the server last read it (`at` 0: never). */
  kube: KubeView
  clock: ClockInfo
  connection: Connection
}

export interface Store {
  getState(): UiState
  subscribe(fn: () => void): () => void
  applyMessage(msg: WsMessage): void
  /** The socket dropped: keep the last view on screen and show that we are reconnecting. */
  setDisconnected(): void
}

const emptyTruth: LabTruth = { vms: [], switches: [], ports: [], partitions: [], vip_holders: [], split_brain_vms: [], at: 0 }
export const emptyKube: KubeView = { at: 0, error: null, workloads: [], pods: [], volumes: [] }

function initialState(): UiState {
  return {
    view: {
      version: 0,
      leader: null,
      term: 0,
      last_sync_at: null,
      hosts: {},
      tokens: {},
      rules: {},
      switches: {},
      networks: {},
      templates: {},
      rollouts: {},
      alerts: {},
      events: [],
      cp: { voters: [], mode: 'Suggest', version: 0 },
      metrics: {},
      deployments: {},
      pods: {},
      vms: {},
      vmStatus: {},
      volumes: {},
      volumeStatus: {},
    },
    truth: emptyTruth,
    kube: emptyKube,
    clock: { now: 0 },
    connection: 'connecting',
  }
}

function connectionOf(view: View, clock: ClockInfo): Connection {
  return view.last_sync_at !== null && clock.now - view.last_sync_at <= STALE_AFTER_MS ? 'live' : 'cp-unavailable'
}

/** A pod's key in `View.pods`, and its id in `ObjectRef`s. */
export function podKey(p: { deployment: string; idx: number }): string {
  return `${p.deployment}/${p.idx}`
}

function byKey<T>(items: T[], key: (t: T) => string): Record<string, T> {
  return Object.fromEntries(items.map((t) => [key(t), t]))
}

function capEvents(events: ClusterEvent[]): ClusterEvent[] {
  return events.length > EVENT_CAP ? events.slice(events.length - EVENT_CAP) : events
}

/** `Object` is the wire type with a "type" tag mixed into the object's own fields. */
function withoutTag<T extends { type: string }>(o: T): Omit<T, 'type'> {
  const { type: _type, ...rest } = o
  return rest
}

function viewFromSnapshot(s: Extract<ServerMsg, { type: 'snapshot' }>['view']): View {
  return {
    version: s.version,
    leader: s.leader,
    term: s.term,
    last_sync_at: s.last_sync_at,
    hosts: byKey(s.hosts, (h) => h.id),
    tokens: byKey(s.tokens, (t) => t.token),
    rules: byKey(s.rules, (r) => r.id),
    switches: byKey(s.switches, (sw) => sw.id),
    networks: byKey(s.networks, (n) => n.id),
    templates: byKey(s.templates, (t) => t.id),
    rollouts: byKey(s.rollouts, (r) => r.id),
    alerts: byKey(s.alerts, (a) => a.id),
    events: capEvents([...s.events].sort((a, b) => a.seq - b.seq)),
    cp: s.cp,
    metrics: s.metrics,
    deployments: byKey(s.deployments, (d) => d.id),
    pods: byKey(s.pods, (p) => podKey(p)),
    vms: byKey(s.vms, (v) => v.id),
    vmStatus: byKey(s.vm_status, (v) => v.id),
    volumes: byKey(s.volumes, (v) => v.id),
    volumeStatus: byKey(s.volume_status, (v) => v.id),
  }
}

/** Applies a delta's upserts and removals, copying only the slices that change. */
function mergeDelta(view: View, d: Extract<ServerMsg, { type: 'delta' }>): View {
  let { hosts, tokens, rules, switches, networks, templates, rollouts, alerts, events, cp, metrics } = view
  let { deployments, pods, vms, vmStatus, volumes, volumeStatus } = view
  const upsertedEvents = new Map<number, ClusterEvent>()
  const removedEvents = new Set<number>()
  const edit = <T,>(rec: Record<string, T>, copied: Set<unknown>): Record<string, T> => {
    if (copied.has(rec)) return rec
    const copy = { ...rec }
    copied.add(copy)
    return copy
  }
  const copied = new Set<unknown>()

  for (const o of d.upserts as ClusterObject[]) {
    switch (o.type) {
      case 'Host':
        hosts = edit(hosts, copied)
        hosts[o.id] = withoutTag(o) as HostObj
        break
      case 'Token':
        tokens = edit(tokens, copied)
        tokens[o.token] = withoutTag(o)
        break
      case 'Rule':
        rules = edit(rules, copied)
        rules[o.id] = withoutTag(o)
        break
      case 'Switch':
        switches = edit(switches, copied)
        switches[o.id] = withoutTag(o)
        break
      case 'Network':
        networks = edit(networks, copied)
        networks[o.id] = withoutTag(o)
        break
      case 'Template':
        templates = edit(templates, copied)
        templates[o.id] = withoutTag(o)
        break
      case 'Rollout':
        rollouts = edit(rollouts, copied)
        rollouts[o.id] = withoutTag(o)
        break
      case 'Alert':
        alerts = edit(alerts, copied)
        alerts[o.id] = withoutTag(o)
        break
      case 'Event':
        upsertedEvents.set(o.seq, withoutTag(o))
        break
      case 'Deployment':
        deployments = edit(deployments, copied)
        deployments[o.id] = withoutTag(o)
        break
      case 'Pod':
        pods = edit(pods, copied)
        pods[podKey(o)] = withoutTag(o)
        break
      case 'Vm':
        vms = edit(vms, copied)
        vms[o.id] = withoutTag(o)
        break
      case 'VmStatus':
        vmStatus = edit(vmStatus, copied)
        vmStatus[o.id] = withoutTag(o)
        break
      case 'Volume':
        volumes = edit(volumes, copied)
        volumes[o.id] = withoutTag(o)
        break
      case 'VolumeStatus':
        volumeStatus = edit(volumeStatus, copied)
        volumeStatus[o.id] = withoutTag(o)
        break
      case 'Cp':
        cp = withoutTag(o)
        break
    }
  }

  for (const r of d.removed as ObjectRef[]) {
    switch (r.kind) {
      case 'Host':
        hosts = edit(hosts, copied)
        delete hosts[r.id]
        metrics = edit(metrics, copied)
        delete metrics[r.id]
        break
      case 'Token':
        tokens = edit(tokens, copied)
        delete tokens[r.id]
        break
      case 'Rule':
        rules = edit(rules, copied)
        delete rules[r.id]
        break
      case 'Switch':
        switches = edit(switches, copied)
        delete switches[r.id]
        break
      case 'Network':
        networks = edit(networks, copied)
        delete networks[r.id]
        break
      case 'Template':
        templates = edit(templates, copied)
        delete templates[r.id]
        break
      case 'Rollout':
        rollouts = edit(rollouts, copied)
        delete rollouts[r.id]
        break
      case 'Alert':
        alerts = edit(alerts, copied)
        delete alerts[r.id]
        break
      case 'Event':
        removedEvents.add(Number(r.id))
        break
      case 'Deployment':
        deployments = edit(deployments, copied)
        delete deployments[r.id]
        break
      case 'Pod':
        pods = edit(pods, copied)
        delete pods[r.id]
        break
      case 'Vm':
        vms = edit(vms, copied)
        delete vms[r.id]
        break
      case 'VmStatus':
        vmStatus = edit(vmStatus, copied)
        delete vmStatus[r.id]
        break
      case 'Volume':
        volumes = edit(volumes, copied)
        delete volumes[r.id]
        break
      case 'VolumeStatus':
        volumeStatus = edit(volumeStatus, copied)
        delete volumeStatus[r.id]
        break
      case 'Cp':
        break
    }
  }

  if (Object.keys(d.metrics).length > 0) metrics = { ...metrics, ...d.metrics }

  if (upsertedEvents.size > 0 || removedEvents.size > 0) {
    const merged = new Map(events.map((e) => [e.seq, e]))
    upsertedEvents.forEach((e, seq) => merged.set(seq, e))
    removedEvents.forEach((seq) => merged.delete(seq))
    events = capEvents([...merged.values()].sort((a, b) => a.seq - b.seq))
  }

  return {
    version: d.version,
    leader: d.leader,
    term: d.term,
    last_sync_at: d.last_sync_at,
    hosts,
    tokens,
    rules,
    switches,
    networks,
    templates,
    rollouts,
    alerts,
    events,
    cp,
    metrics,
    deployments,
    pods,
    vms,
    vmStatus,
    volumes,
    volumeStatus,
  }
}

export function createStore(): Store {
  let state = initialState()
  const listeners = new Set<() => void>()
  const set = (next: UiState) => {
    state = next
    listeners.forEach((fn) => fn())
  }

  return {
    getState: () => state,
    subscribe(fn) {
      listeners.add(fn)
      return () => {
        listeners.delete(fn)
      }
    },
    applyMessage(msg) {
      if (msg.type === 'snapshot') {
        const view = viewFromSnapshot(msg.view)
        set({ view, truth: msg.truth, kube: msg.kube, clock: msg.clock, connection: connectionOf(view, msg.clock) })
      } else {
        const view = mergeDelta(state.view, msg)
        set({ view, truth: msg.truth, kube: msg.kube ?? state.kube, clock: msg.clock, connection: connectionOf(view, msg.clock) })
      }
    },
    setDisconnected() {
      set({ ...state, connection: 'connecting' })
    },
  }
}

/** The app's one store, fed by `connect()` in ws.ts. */
export const store = createStore()

/** Subscribes to a slice of the app's store. The selector must return an existing value, not build a new one each call. */
export function useStore<T>(selector: (s: UiState) => T): T {
  return useSyncExternalStore(store.subscribe, () => selector(store.getState()))
}
