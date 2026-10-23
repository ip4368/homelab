import type { Deployment } from './gen/Deployment'
import type { GroundTruth } from './gen/GroundTruth'
import type { HostObj } from './gen/HostObj'
import type { ReplicaState } from './gen/ReplicaState'
import type { Vm } from './gen/Vm'
import type { VmPhase } from './gen/VmPhase'
import type { Volume } from './gen/Volume'
import type { View } from './store'

/** What the backend accepts (model::workload). */
export const MAX_DEPLOYMENT_REPLICAS = 1000
export const MAX_VOLUME_REPLICAS = 5

export type WorkloadKind = 'vm' | 'pod' | 'volume'

export const KIND_LABELS: Record<WorkloadKind, string> = { vm: 'VM', pod: 'Pod', volume: 'Volume' }

/** The slices of the view that describe workloads. */
export type WorkloadSource = Pick<View, 'deployments' | 'pods' | 'vms' | 'vmStatus' | 'volumes' | 'volumeStatus'>

/** One row of the workloads table. */
export interface WorkloadItem {
  kind: WorkloadKind
  /** A pod's id is `<deployment>/<idx>`. */
  id: string
  /** Hosts the workload is placed on: a migrating VM lists source then target; a volume lists its replicas' hosts. */
  hosts: string[]
  /** VM or pod phase, or volume health. */
  health: string
  detail: string
}

export interface WorkloadFilter {
  kind: WorkloadKind | 'all'
  text: string
  /** Empty for any host. */
  host: string
  /** Empty for any health. */
  health: string
}

export function vmPhaseName(p: VmPhase): string {
  return typeof p === 'string' ? p : 'Pending' in p ? 'Pending' : 'Migrating'
}

/** The text after the phase name: why it is pending, or where it is migrating to. */
export function vmPhaseNote(p: VmPhase): string {
  if (typeof p === 'string') return ''
  return 'Pending' in p ? p.Pending : `to ${p.Migrating.to}`
}

export function podPhaseName(p: { Pending: string } | 'Running'): string {
  return typeof p === 'string' ? p : 'Pending'
}

export function replicaStateLabel(s: ReplicaState): string {
  return typeof s === 'string' ? s : `Rebuilding ${Math.round(s.Rebuilding.pct)}%`
}

/** The css class for a phase, replica state or health word. */
export function healthTone(health: string): 'ok' | 'warn' | 'bad' {
  if (health === 'Running' || health === 'Healthy') return 'ok'
  if (health === 'Faulted' || health === 'Stopped' || health === 'Stale') return 'bad'
  return 'warn'
}

function unique(items: string[]): string[] {
  return [...new Set(items)]
}

const byId = (a: WorkloadItem, b: WorkloadItem) => a.id.localeCompare(b.id, undefined, { numeric: true })

/** Every VM, pod and volume as table rows: VMs, then pods, then volumes, each by id. */
export function buildWorkloads(src: WorkloadSource): WorkloadItem[] {
  const vms: WorkloadItem[] = Object.values(src.vms).map((vm) => {
    const st = src.vmStatus[vm.id]
    const phase = st ? vmPhaseName(st.phase) : 'Pending'
    const note = st ? vmPhaseNote(st.phase) : ''
    const hosts = st ? unique([...(st.host ? [st.host] : []), ...(typeof st.phase === 'object' && 'Migrating' in st.phase ? [st.phase.Migrating.to] : [])]) : []
    const detail = [`${vm.vcpu} vCPU`, `${vm.ram_mb} MB`, `volume ${vm.volume}`, note].filter(Boolean).join(' · ')
    return { kind: 'vm', id: vm.id, hosts, health: phase, detail }
  })
  const pods: WorkloadItem[] = Object.values(src.pods).map((p) => {
    const id = `${p.deployment}/${p.idx}`
    const note = typeof p.phase === 'object' ? p.phase.Pending : ''
    return { kind: 'pod', id, hosts: p.host ? [p.host] : [], health: podPhaseName(p.phase), detail: [src.deployments[p.deployment]?.image ?? '', note].filter(Boolean).join(' · ') }
  })
  const volumes: WorkloadItem[] = Object.values(src.volumes).map((v) => {
    const st = src.volumeStatus[v.id]
    const replicas = st?.replicas ?? []
    const healthy = replicas.filter((r) => r.state === 'Healthy').length
    return {
      kind: 'volume',
      id: v.id,
      hosts: unique(replicas.map((r) => r.host)),
      health: st?.health ?? 'Faulted',
      detail: `${v.size_gb} GB · ${healthy}/${v.replicas} replicas healthy`,
    }
  })
  return [...vms.sort(byId), ...pods.sort(byId), ...volumes.sort(byId)]
}

export function filterWorkloads(items: WorkloadItem[], f: WorkloadFilter): WorkloadItem[] {
  const text = f.text.trim().toLowerCase()
  return items.filter(
    (i) =>
      (f.kind === 'all' || i.kind === f.kind) &&
      (f.health === '' || i.health === f.health) &&
      (f.host === '' || i.hosts.includes(f.host)) &&
      (text === '' || i.id.toLowerCase().includes(text)),
  )
}

export interface TruthDiffEntry {
  /** A host id; for a split-brain VM `vm:<id>`. */
  hostId: string
  believed: string
  actual: string
}

/** Lifecycles in which the control plane takes a host to be up and heartbeating. */
const BELIEVED_UP = new Set<HostObj['lifecycle']>(['Ready', 'Cordoned', 'Draining', 'Drained'])

/**
 * Where the control plane's view and the simulator's truth disagree: hosts the
 * view takes to be up that are powered off, hosts it has as NotReady that are
 * powered (cut off, not dead), and VMs running in more than one place.
 */
export function truthDiff(view: Pick<View, 'hosts'>, truth: Pick<GroundTruth, 'hosts' | 'split_brain_vms'>): TruthDiffEntry[] {
  const out: TruthDiffEntry[] = []
  for (const t of truth.hosts) {
    const h = view.hosts[t.id]
    if (!h) continue
    if (BELIEVED_UP.has(h.lifecycle) && !t.powered) out.push({ hostId: t.id, believed: h.lifecycle, actual: 'powered off' })
    else if (h.lifecycle === 'NotReady' && t.powered) out.push({ hostId: t.id, believed: h.lifecycle, actual: 'powered on' })
  }
  for (const vm of truth.split_brain_vms) {
    out.push({ hostId: `vm:${vm}`, believed: 'runs on one host', actual: 'split brain: running on several hosts' })
  }
  return out
}

export interface HostWorkloads {
  vms: string[]
  pods: string[]
  replicas: { volume: string; disk: string; state: ReplicaState }[]
}

/** What the view places on `host`. A VM migrating to the host counts, as it is about to run there. */
export function hostWorkloads(src: Pick<WorkloadSource, 'pods' | 'vmStatus' | 'volumeStatus'>, host: string): HostWorkloads {
  const vms = Object.values(src.vmStatus)
    .filter((st) => st.host === host || (typeof st.phase === 'object' && 'Migrating' in st.phase && st.phase.Migrating.to === host))
    .map((st) => st.id)
    .sort()
  const pods = Object.values(src.pods)
    .filter((p) => p.host === host)
    .map((p) => `${p.deployment}/${p.idx}`)
    .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }))
  const replicas = Object.values(src.volumeStatus)
    .flatMap((v) => v.replicas.filter((r) => r.host === host).map((r) => ({ volume: v.id, disk: r.disk, state: r.state })))
    .sort((a, b) => a.volume.localeCompare(b.volume))
  return { vms, pods, replicas }
}

/** Hosts a VM may be migrated to: Ready, with the Vm role, and not the host it is on. */
export function migrationTargets(hosts: Record<string, HostObj>, from: string | null): string[] {
  return Object.values(hosts)
    .filter((h) => h.lifecycle === 'Ready' && h.roles.includes('Vm') && h.id !== from)
    .map((h) => h.id)
    .sort()
}

/** What the workload drawer shows: pods belong to their deployment. */
export interface WorkloadRef {
  kind: 'vm' | 'deployment' | 'volume'
  id: string
}

export function refOfPod(podId: string): WorkloadRef {
  return { kind: 'deployment', id: podId.slice(0, podId.lastIndexOf('/')) }
}

export function refOf(item: Pick<WorkloadItem, 'kind' | 'id'>): WorkloadRef {
  return item.kind === 'pod' ? refOfPod(item.id) : { kind: item.kind, id: item.id }
}

function checkName(id: string, existing: string[]): string | null {
  if (id.trim() === '') return 'name is required'
  if (existing.includes(id.trim())) return `${id.trim()} already exists`
  return null
}

/** The first thing wrong with a new deployment, worded like the backend's refusal; null when fine. */
export function validateDeployment(d: Deployment, existing: string[]): string | null {
  return (
    checkName(d.id, existing) ??
    (d.image.trim() === '' ? 'image is required' : null) ??
    (!(d.replicas >= 1 && d.replicas <= MAX_DEPLOYMENT_REPLICAS) ? `replicas must be 1 to ${MAX_DEPLOYMENT_REPLICAS}, not ${d.replicas}` : null) ??
    (d.cpu_m < 1 ? 'cpu_m must be at least 1' : null) ??
    (d.ram_mb < 1 ? 'ram_mb must be at least 1' : null)
  )
}

export function validateVolume(v: Volume, existing: string[]): string | null {
  return (
    checkName(v.id, existing) ??
    (v.size_gb < 1 ? 'size_gb must be at least 1' : null) ??
    (!(v.replicas >= 1 && v.replicas <= MAX_VOLUME_REPLICAS) ? `replicas must be 1 to ${MAX_VOLUME_REPLICAS}, not ${v.replicas}` : null)
  )
}

/** `volumes` are the ids that exist: a VM's disk volume must be created first. */
export function validateVm(vm: Vm, existing: string[], volumes: string[]): string | null {
  return (
    checkName(vm.id, existing) ??
    (vm.vcpu < 1 ? 'vcpu must be at least 1' : null) ??
    (vm.ram_mb < 1 ? 'ram_mb must be at least 1' : null) ??
    (vm.disk_gb < 1 ? 'disk_gb must be at least 1' : null) ??
    (!volumes.includes(vm.volume) ? 'pick an existing volume for the disk' : null)
  )
}
