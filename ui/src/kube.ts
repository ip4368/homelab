import type { KubePod } from './gen/KubePod'
import type { KubeView } from './gen/KubeView'
import type { KubeWorkload } from './gen/KubeWorkload'
import type { LonghornReplica } from './gen/LonghornReplica'
import type { LonghornVolume } from './gen/LonghornVolume'

/** Namespaces K3s and Longhorn run their own pods in. */
export const SYSTEM_NAMESPACES = ['kube-system', 'longhorn-system']

export interface KubeFilter {
  /** Empty for any namespace. */
  namespace: string
  /** Leave out SYSTEM_NAMESPACES (unless one is picked). */
  hideSystem: boolean
  /** Empty for any host. */
  host: string
  text: string
}

export type Tone = 'ok' | 'warn' | 'bad' | 'muted'

/** A pod's colour: running with every container ready, on its way, finished, or in trouble. */
export function podTone(p: Pick<KubePod, 'status' | 'ready' | 'containers'>): Tone {
  if (p.status === 'Running') return p.ready === p.containers ? 'ok' : 'warn'
  if (p.status === 'Succeeded' || p.status === 'Completed') return 'muted'
  if (['Pending', 'ContainerCreating', 'PodInitializing', 'Terminating'].includes(p.status)) return 'warn'
  return 'bad'
}

export function workloadTone(w: Pick<KubeWorkload, 'ready' | 'desired'>): Tone {
  if (w.desired === 0) return 'muted'
  return w.ready >= w.desired ? 'ok' : w.ready === 0 ? 'bad' : 'warn'
}

export function volumeTone(v: Pick<LonghornVolume, 'robustness' | 'state'>): Tone {
  if (v.robustness === 'healthy') return 'ok'
  if (v.robustness === 'degraded') return 'warn'
  if (v.robustness === 'faulted') return 'bad'
  return v.state === 'detached' ? 'muted' : 'warn'
}

/** What a replica is doing, in words: `in sync`, `rebuilding`, `failed`… */
export function replicaLabel(r: LonghornReplica): { text: string; tone: Tone } {
  if (r.failed) return { text: 'failed', tone: 'bad' }
  if (r.mode === 'RW') return { text: 'in sync', tone: 'ok' }
  if (r.mode === 'WO') return { text: 'rebuilding', tone: 'warn' }
  if (r.mode === 'ERR') return { text: 'error', tone: 'bad' }
  return { text: r.state, tone: r.state === 'running' || r.state === 'stopped' ? 'muted' : 'warn' }
}

/** The hosts a workload's pods run on. */
export function workloadHosts(w: KubeWorkload, pods: KubePod[]): string[] {
  const owner = `${w.kind}/${w.name}`
  return [...new Set(pods.filter((p) => p.namespace === w.namespace && p.owner === owner && p.node).map((p) => p.node as string))].sort()
}

/** The hosts a volume is on: where it is attached, and where its replicas are. */
export function volumeHosts(v: LonghornVolume): string[] {
  return [...new Set([...(v.node ? [v.node] : []), ...v.replicas.map((r) => r.node)])].sort()
}

function volumeNamespace(v: LonghornVolume): string {
  return v.pvc ? v.pvc.slice(0, v.pvc.indexOf('/')) : ''
}

export interface KubeRows {
  workloads: { workload: KubeWorkload; hosts: string[] }[]
  pods: KubePod[]
  volumes: LonghornVolume[]
}

/** The rows the filter keeps. */
export function filterKube(k: Pick<KubeView, 'workloads' | 'pods' | 'volumes'>, f: KubeFilter): KubeRows {
  const text = f.text.trim().toLowerCase()
  const nsOk = (ns: string) => (f.namespace ? ns === f.namespace : !(f.hideSystem && SYSTEM_NAMESPACES.includes(ns)))
  const textOk = (...names: (string | null)[]) => text === '' || names.some((n) => n?.toLowerCase().includes(text))
  const workloads = k.workloads
    .map((workload) => ({ workload, hosts: workloadHosts(workload, k.pods) }))
    .filter(({ workload: w, hosts }) => nsOk(w.namespace) && (f.host === '' || hosts.includes(f.host)) && textOk(w.name))
  const pods = k.pods.filter((p) => nsOk(p.namespace) && (f.host === '' || p.node === f.host) && textOk(p.name, p.owner))
  const volumes = k.volumes.filter(
    (v) => nsOk(volumeNamespace(v)) && (f.host === '' || volumeHosts(v).includes(f.host)) && textOk(v.name, v.pvc, ...v.pods),
  )
  return { workloads, pods, volumes }
}

/** kubectl's AGE: `45s`, `12m`, `3h`, `2d`. */
export function age(since: number | null, now: number): string {
  if (since === null) return ''
  const s = Math.max(0, Math.floor((now - since) / 1000))
  if (s < 120) return `${s}s`
  if (s < 2 * 3600) return `${Math.floor(s / 60)}m`
  if (s < 2 * 86400) return `${Math.floor(s / 3600)}h`
  return `${Math.floor(s / 86400)}d`
}

export function gib(bytes: number): string {
  const g = bytes / 2 ** 30
  return `${Number.isInteger(g) ? g : g.toFixed(1)} GiB`
}

/** The pods on `host`, by namespace and name. */
export function podsOn(k: Pick<KubeView, 'pods'>, host: string): KubePod[] {
  return k.pods.filter((p) => p.node === host)
}
