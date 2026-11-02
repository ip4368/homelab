import type { ApplyOutcome } from './gen/ApplyOutcome'
import type { ApplyReport } from './gen/ApplyReport'
import type { FieldConflict } from './gen/FieldConflict'
import type { KubePod } from './gen/KubePod'
import type { KubeView } from './gen/KubeView'
import type { KubeWorkload } from './gen/KubeWorkload'
import type { LonghornReplica } from './gen/LonghornReplica'
import type { LonghornVolume } from './gen/LonghornVolume'

/** Namespaces K3s and Longhorn run their own pods in. */
export const SYSTEM_NAMESPACES = ['kube-system', 'longhorn-system']

/** Namespaces the server changes nothing in (as `PROTECTED_NAMESPACES` in crates/server/src/kube_action.rs): kubectl only. */
export const PROTECTED_NAMESPACES = [...SYSTEM_NAMESPACES, 'kube-public', 'kube-node-lease']

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

/**
 * The pods `kubectl drain` still has to move off a host: all but DaemonSet
 * pods, static pods (owned by their node) and pods that finished.
 */
export function podsToDrain(pods: KubePod[]): KubePod[] {
  const stays = (p: KubePod) => p.owner?.startsWith('DaemonSet/') || p.owner?.startsWith('Node/')
  return pods.filter((p) => !stays(p) && !['Succeeded', 'Failed', 'Completed'].includes(p.status))
}

/** The confirmation before removing `host`, given the pods on it. */
export function removeHostPrompt(host: string, pods: KubePod[]): string {
  const left = podsToDrain(pods).length
  const what = left === 0 ? 'No pods need moving off it.' : `${left} pod${left === 1 ? '' : 's'} still run${left === 1 ? 's' : ''} on it: drain it first so they move off cleanly.`
  return `Remove ${host}? ${what} It leaves the cluster for good; to bring it back, reinstall it with a new join token.`
}

/** What the Workloads tab offers to do to `w`: nothing in a protected namespace, and no scaling of a DaemonSet (one pod per node). */
export function workloadActions(w: Pick<KubeWorkload, 'namespace' | 'kind'>): { scale: boolean; restart: boolean; edit: boolean; remove: boolean } {
  if (PROTECTED_NAMESPACES.includes(w.namespace)) return { scale: false, restart: false, edit: false, remove: false }
  return { scale: w.kind !== 'DaemonSet', restart: true, edit: true, remove: true }
}

/** The claims of a StatefulSet's pods: the Longhorn volumes its pods use. */
export function workloadClaims(w: KubeWorkload, pods: KubePod[], volumes: LonghornVolume[]): string[] {
  const owner = `${w.kind}/${w.name}`
  const mine = new Set(pods.filter((p) => p.namespace === w.namespace && p.owner === owner).map((p) => p.name))
  return volumes.filter((v) => v.pvc && volumeNamespace(v) === w.namespace && v.pods.some((p) => mine.has(p))).map((v) => v.pvc as string).sort()
}

export function restartPrompt(w: KubeWorkload): string {
  return `Restart ${w.kind} ${w.namespace}/${w.name}? Kubernetes replaces its pods a few at a time, as kubectl rollout restart does.`
}

export function deletePrompt(w: KubeWorkload, claims: string[]): string {
  const ask = `Delete ${w.kind} ${w.namespace}/${w.name} and its pods?`
  if (w.kind !== 'StatefulSet') return ask
  const kept = claims.length === 0 ? 'Its volume claims stay' : `Its volume claims stay, with their data: ${claims.join(', ')}`
  return `${ask} ${kept}. Delete them with kubectl once you no longer need them.`
}

/** What became of an applied object, in words, as done or (in a dry run) as it would be. */
export function outcomeLabel(o: ApplyOutcome, dryRun: boolean): { text: string; tone: Tone } {
  if (o === 'Created') return { text: dryRun ? 'will be created' : 'created', tone: 'ok' }
  if (o === 'Configured') return { text: dryRun ? 'will change' : 'changed', tone: 'ok' }
  if (o === 'Unchanged') return { text: 'unchanged', tone: 'muted' }
  if (o === 'AfterNamespace') return { text: 'checked once its namespace is created', tone: 'muted' }
  if (o === 'Skipped') return { text: 'not tried: stopped at the object above', tone: 'muted' }
  if ('Failed' in o) return { text: o.Failed.message, tone: 'bad' }
  return { text: 'conflict', tone: 'warn' }
}

/**
 * What a preview allows. Applying, if nothing failed or conflicted (a preview
 * stops at the first, so what follows went unchecked). And the fields other
 * managers own: previewing again with `force` takes them over.
 */
export function previewVerdict(r: ApplyReport): { canApply: boolean; conflicts: (FieldConflict & { object: string })[] } {
  const failed = r.objects.some((o) => typeof o.outcome === 'object' && 'Failed' in o.outcome)
  const conflicts = r.objects.flatMap((o) =>
    typeof o.outcome === 'object' && 'Conflict' in o.outcome ? o.outcome.Conflict.conflicts.map((c) => ({ ...c, object: `${o.kind} ${o.name}` })) : [],
  )
  return { canApply: !failed && conflicts.length === 0, conflicts }
}

/** The Apply dialog's start for editing `w`, whose live manifest is `yaml`: an edit takes over what it changes, as `kubectl edit` does. */
export function editDraft(w: KubeWorkload, yaml: string): { title: string; yaml: string; namespace: string; force: boolean; note: string } {
  return {
    title: `Edit ${w.kind} ${w.namespace}/${w.name}`,
    yaml,
    namespace: w.namespace,
    force: true,
    note: 'As kubectl edit: what you change is set, whoever set it before (a scale from this page, kubectl). Untick "take over" to see who that was instead.',
  }
}
