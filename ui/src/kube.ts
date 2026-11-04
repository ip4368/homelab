import type { ApplyOutcome } from './gen/ApplyOutcome'
import type { ApplyReport } from './gen/ApplyReport'
import type { FieldConflict } from './gen/FieldConflict'
import type { HealthState } from './gen/HealthState'
import type { KubeEvent } from './gen/KubeEvent'
import type { KubePod } from './gen/KubePod'
import type { KubeView } from './gen/KubeView'
import type { KubeWorkload } from './gen/KubeWorkload'
import type { LonghornReplica } from './gen/LonghornReplica'
import type { LonghornVolume } from './gen/LonghornVolume'
import type { Mount } from './gen/Mount'
import type { Pdb } from './gen/Pdb'
import { formatBps } from './metrics'

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
  return workloadVolumes(w, pods, volumes).flatMap((v) => (v.pvc ? [v.pvc] : [])).sort()
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

/** Millicores as people read them: `850m`, `1.25 cores`; a dash for none. */
export function cpu(milli: number | null | undefined): string {
  if (milli === null || milli === undefined) return '–'
  if (milli < 1000) return `${milli}m`
  return `${(milli / 1000).toFixed(milli < 10_000 ? 2 : 1)} cores`
}

/** Bytes as Kubernetes writes memory: `84Mi`, `1.2Gi`; a dash for none. */
export function mem(bytes: number | null | undefined): string {
  if (bytes === null || bytes === undefined) return '–'
  if (bytes < 2 ** 30) return `${Math.round(bytes / 2 ** 20)}Mi`
  return `${(bytes / 2 ** 30).toFixed(1)}Gi`
}

/** Usage against a limit, when there is one: `84Mi / 2Gi`. */
export function usage(used: number | null, limit: number | null, format: (n: number | null) => string): string {
  return limit === null ? format(used) : `${format(used)} / ${format(limit)}`
}

/** Usage near or over its limit is worth a look. */
export function usageTone(used: number | null, limit: number | null): Tone {
  if (used === null || limit === null || limit === 0) return 'muted'
  return used >= limit * 0.9 ? 'bad' : used >= limit * 0.75 ? 'warn' : 'ok'
}

export const HEALTH_LABELS: Record<HealthState, string> = { Healthy: 'healthy', RollingOut: 'rolling out', Degraded: 'degraded', Down: 'down', Idle: 'idle' }

export function healthTone(s: HealthState): Tone {
  return s === 'Healthy' ? 'ok' : s === 'Down' ? 'bad' : s === 'Idle' ? 'muted' : 'warn'
}

/** The Warning events about `w`: on it, its ReplicaSets or its pods (the server matched them). */
export function workloadEvents(w: Pick<KubeWorkload, 'namespace' | 'kind' | 'name'>, events: KubeEvent[]): KubeEvent[] {
  const owner = `${w.kind}/${w.name}`
  return events.filter((e) => e.namespace === w.namespace && e.workload === owner)
}

/** The Longhorn volumes `w`'s pods use. */
export function workloadVolumes(w: Pick<KubeWorkload, 'namespace' | 'kind' | 'name'>, pods: KubePod[], volumes: LonghornVolume[]): LonghornVolume[] {
  const owner = `${w.kind}/${w.name}`
  const mine = new Set(pods.filter((p) => p.namespace === w.namespace && p.owner === owner).map((p) => p.name))
  return volumes.filter((v) => volumeNamespace(v) === w.namespace && v.pods.some((p) => mine.has(p)))
}

/** Why a pod is as it is, for its status's tooltip: Kubernetes' words, and how a container last ended. */
export function podWhy(p: Pick<KubePod, 'detail' | 'last_termination' | 'restarts'>): string | undefined {
  const parts = [p.detail, p.last_termination && p.restarts > 0 ? `last ended: ${p.last_termination.reason} (exit ${p.last_termination.exit_code})` : null]
  return parts.filter(Boolean).join('\n') || undefined
}

/** A workload's key, as the drawer is opened with: `<namespace>/<kind>/<name>`. */
export function workloadKey(w: Pick<KubeWorkload, 'namespace' | 'kind' | 'name'>): string {
  return `${w.namespace}/${w.kind}/${w.name}`
}

/** An image digest cut short as `sha256:4f1c9a2b7d3e`, from a container's `imageID` (`docker.io/library/nginx@sha256:…`). */
export function shortDigest(imageId: string): string {
  const at = imageId.lastIndexOf('@')
  const digest = at >= 0 ? imageId.slice(at + 1) : imageId
  const colon = digest.indexOf(':')
  return colon >= 0 ? `${digest.slice(0, colon)}:${digest.slice(colon + 1, colon + 13)}` : digest.slice(0, 12)
}

/** The digests each container of `w` runs, across its pods: more than one means a tag moved under it. */
export function containerDigests(w: Pick<KubeWorkload, 'namespace' | 'kind' | 'name'>, pods: KubePod[]): Map<string, string[]> {
  const owner = `${w.kind}/${w.name}`
  const out = new Map<string, string[]>()
  for (const p of pods.filter((p) => p.namespace === w.namespace && p.owner === owner)) {
    for (const s of p.container_states) {
      if (!s.image_id) continue
      const seen = out.get(s.name) ?? []
      if (!seen.includes(s.image_id)) out.set(s.name, [...seen, s.image_id])
    }
  }
  return out
}

/** A container's request and limit: `100m / 1.00 cores`, `64Mi / –` with no limit, nothing with neither. */
export function requestLimit(request: number | null, limit: number | null, format: (n: number | null) => string): string {
  if (request === null && limit === null) return ''
  return `${format(request)} / ${format(limit)}`
}

/** Where a mount comes from, in a few words: `claim data-0`, `ConfigMap app (keys a)`, `host /dev/ttyUSB0`. */
export function mountSource(m: Mount): string {
  const kind: Record<Mount['kind'], string> = {
    Claim: 'claim',
    ConfigMap: 'ConfigMap',
    Secret: 'Secret',
    EmptyDir: 'emptyDir',
    HostPath: 'host',
    Projected: 'projected',
    DownwardApi: 'downward API',
    Other: 'volume',
  }
  const main = [kind[m.kind], m.source].filter(Boolean).join(' ')
  return m.detail ? `${main} (${m.detail})` : main
}

/** The Longhorn volume behind claim `claim` in `namespace`, if it is one. */
export function claimVolume(namespace: string, claim: string, volumes: LonghornVolume[]): LonghornVolume | undefined {
  return volumes.find((v) => v.pvc === `${namespace}/${claim}`)
}

/** A disruption budget worth a warning: one that lets no pod be evicted now, so draining their hosts waits. */
export function pdbWarning(pdb: Pdb | null): string | null {
  if (!pdb || pdb.disruptions_allowed > 0) return null
  return `PodDisruptionBudget ${pdb.name} allows no disruption now (${pdb.healthy}/${pdb.desired_healthy} healthy): draining its pods' hosts waits for it`
}

/** Network receive and send rates (bits per second): `↓ 1.2 Mb/s ↑ 80 kb/s`; a dash before there are any. */
export function netRate(rx: number | null, tx: number | null): string {
  if (rx === null && tx === null) return '–'
  return `↓ ${formatBps(rx)} ↑ ${formatBps(tx)}`
}

/** What a volume's filesystem holds of its size: `7.2 / 10 GiB`, `850Mi / 2 GiB`; a dash when unknown. */
export function bytesUsed(used: number | null, size: number | null): string {
  if (used === null) return '–'
  const size_ = size === null ? '' : ` / ${gib(size)}`
  return used >= 2 ** 30 && size !== null ? `${(used / 2 ** 30).toFixed(1)}${size_}` : `${mem(used)}${size_}`
}

/** A volume nearly full is worth a look: from 85 %, and past 95 % it is about to fail writes. */
export function usedTone(used: number | null, size: number | null): Tone {
  if (used === null || size === null || size === 0) return 'muted'
  return used >= size * 0.95 ? 'bad' : used >= size * 0.85 ? 'warn' : 'ok'
}
