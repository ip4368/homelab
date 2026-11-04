import type { Alert } from './gen/Alert'
import type { HostObj } from './gen/HostObj'
import type { KubeView } from './gen/KubeView'
import type { KubeWorkload } from './gen/KubeWorkload'
import type { LabTruth } from './gen/LabTruth'
import type { MetricSample } from './gen/MetricSample'
import type { SwitchObj } from './gen/SwitchObj'
import { workloadKey } from './kube'

export type OverviewTarget =
  | { kind: 'host'; id: string }
  | { kind: 'workload'; key: string }
  | { kind: 'switch'; id: string }
  | { kind: 'activity' }
  | { kind: 'fleet' }
  | { kind: 'workloads' }
  | { kind: 'topology' }

export interface AttentionItem {
  id: string
  severity: 'bad' | 'warn'
  title: string
  detail: string
  target: OverviewTarget
}

export interface OverviewInput {
  hosts: HostObj[]
  metrics: Record<string, MetricSample>
  alerts: Alert[]
  switches: SwitchObj[]
  kube: KubeView
  truth: LabTruth
}

export interface HostLoad {
  id: string
  lifecycle: HostObj['lifecycle']
  cpu: number | null
  ram: number | null
}

export interface OverviewSummary {
  hosts: {
    total: number
    ready: number
    notReady: number
    pending: number
    other: number
    sampled: number
    avgCpu: number | null
    avgRam: number | null
    busiest: HostLoad[]
  }
  workloads: {
    total: number
    healthy: number
    rolling: number
    degraded: number
    down: number
    idle: number
    pods: number
    readyPods: number
    restartingPods: number
    cpuMilli: number
    memoryBytes: number
    netRxBps: number
    netTxBps: number
    attentionFirst: KubeWorkload[]
  }
  storage: {
    volumes: number
    degraded: number
    faulted: number
    detached: number
    claims: number
    pendingClaims: number
  }
  infrastructure: {
    switches: number
    kubeNodes: number
    readyKubeNodes: number
    pressures: { node: string; pressure: string }[]
    labVmsRunning: number
    labVms: number
    partitions: number
    vipHolders: string[]
  }
  alerts: { active: number; critical: number; warning: number }
  attention: AttentionItem[]
}

function average(values: number[]): number | null {
  return values.length === 0 ? null : values.reduce((a, b) => a + b, 0) / values.length
}

function workloadRank(w: KubeWorkload): number {
  return w.health.state === 'Down' ? 0 : w.health.state === 'Degraded' ? 1 : w.health.state === 'RollingOut' ? 2 : w.health.state === 'Idle' ? 3 : 4
}

/** Everything the Operations home needs, worked out once from the live view. */
export function summarizeOverview(input: OverviewInput): OverviewSummary {
  const hostLoads = input.hosts.map((h) => {
    const metric = input.metrics[h.id]
    return { id: h.id, lifecycle: h.lifecycle, cpu: metric?.cpu_pct ?? null, ram: metric?.ram_pct ?? null }
  })
  const sampled = hostLoads.filter((h) => h.cpu !== null || h.ram !== null)
  const attention: AttentionItem[] = []

  if (input.kube.error) {
    attention.push({ id: 'kube-read', severity: 'warn', title: 'Kubernetes view is stale', detail: input.kube.error, target: { kind: 'workloads' } })
  }
  for (const a of input.alerts.filter((x) => x.resolved_at === null)) {
    const target: OverviewTarget =
      a.entity.kind === 'Host' ? { kind: 'host', id: a.entity.id } : a.entity.kind === 'Switch' ? { kind: 'switch', id: a.entity.id } : { kind: 'activity' }
    attention.push({
      id: `alert-${a.id}`,
      severity: a.severity === 'Critical' ? 'bad' : 'warn',
      title: `${a.kind} · ${a.entity.id}`,
      detail: a.message,
      target,
    })
  }
  for (const h of input.hosts.filter((x) => x.lifecycle === 'NotReady')) {
    attention.push({ id: `host-${h.id}`, severity: 'bad', title: `${h.id} is NotReady`, detail: h.roles.length ? h.roles.join(', ') : h.location, target: { kind: 'host', id: h.id } })
  }
  for (const w of input.kube.workloads.filter((x) => x.health.state === 'Down' || x.health.state === 'Degraded')) {
    attention.push({
      id: `workload-${workloadKey(w)}`,
      severity: w.health.state === 'Down' ? 'bad' : 'warn',
      title: `${w.namespace}/${w.name} is ${w.health.state.toLowerCase()}`,
      detail: w.health.reason ?? `${w.ready}/${w.desired} ready`,
      target: { kind: 'workload', key: workloadKey(w) },
    })
  }
  const pressures = input.kube.nodes.flatMap((n) => n.pressures.map((pressure) => ({ node: n.name, pressure })))
  for (const p of pressures) {
    attention.push({ id: `pressure-${p.node}-${p.pressure}`, severity: 'warn', title: `${p.node} has ${p.pressure}`, detail: 'Kubernetes node pressure', target: { kind: 'host', id: p.node } })
  }
  for (const v of input.kube.volumes.filter((x) => x.robustness === 'faulted' || x.robustness === 'degraded')) {
    attention.push({
      id: `volume-${v.name}`,
      severity: v.robustness === 'faulted' ? 'bad' : 'warn',
      title: `${v.pvc ?? v.name} is ${v.robustness}`,
      detail: `${v.replicas.filter((r) => r.mode === 'RW').length}/${v.replicas_wanted} replicas in sync`,
      target: { kind: 'workloads' },
    })
  }
  if (input.truth.vip_holders.length > 1) {
    attention.push({ id: 'split-vip', severity: 'bad', title: 'VIP has multiple holders', detail: input.truth.vip_holders.join(', '), target: { kind: 'fleet' } })
  }

  const seen = new Set<string>()
  const deduped = attention.filter((a) => (seen.has(a.id) ? false : (seen.add(a.id), true)))
  deduped.sort((a, b) => (a.severity === b.severity ? a.title.localeCompare(b.title) : a.severity === 'bad' ? -1 : 1))

  return {
    hosts: {
      total: input.hosts.length,
      ready: input.hosts.filter((h) => h.lifecycle === 'Ready').length,
      notReady: input.hosts.filter((h) => h.lifecycle === 'NotReady').length,
      pending: input.hosts.filter((h) => h.lifecycle === 'Pending').length,
      other: input.hosts.filter((h) => !['Ready', 'NotReady', 'Pending'].includes(h.lifecycle)).length,
      sampled: sampled.length,
      avgCpu: average(sampled.flatMap((h) => (h.cpu === null ? [] : [h.cpu]))),
      avgRam: average(sampled.flatMap((h) => (h.ram === null ? [] : [h.ram]))),
      busiest: [...hostLoads].sort((a, b) => Math.max(b.cpu ?? -1, b.ram ?? -1) - Math.max(a.cpu ?? -1, a.ram ?? -1)).slice(0, 6),
    },
    workloads: {
      total: input.kube.workloads.length,
      healthy: input.kube.workloads.filter((w) => w.health.state === 'Healthy').length,
      rolling: input.kube.workloads.filter((w) => w.health.state === 'RollingOut').length,
      degraded: input.kube.workloads.filter((w) => w.health.state === 'Degraded').length,
      down: input.kube.workloads.filter((w) => w.health.state === 'Down').length,
      idle: input.kube.workloads.filter((w) => w.health.state === 'Idle').length,
      pods: input.kube.pods.length,
      readyPods: input.kube.pods.filter((p) => p.ready > 0 && p.ready === p.containers).length,
      restartingPods: input.kube.pods.filter((p) => p.restarts > 0).length,
      cpuMilli: input.kube.workloads.reduce((n, w) => n + (w.cpu_milli ?? 0), 0),
      memoryBytes: input.kube.workloads.reduce((n, w) => n + (w.memory_bytes ?? 0), 0),
      netRxBps: input.kube.workloads.reduce((n, w) => n + (w.net_rx_bps ?? 0), 0),
      netTxBps: input.kube.workloads.reduce((n, w) => n + (w.net_tx_bps ?? 0), 0),
      attentionFirst: [...input.kube.workloads].sort((a, b) => workloadRank(a) - workloadRank(b) || (b.cpu_milli ?? 0) - (a.cpu_milli ?? 0)).slice(0, 7),
    },
    storage: {
      volumes: input.kube.volumes.length,
      degraded: input.kube.volumes.filter((v) => v.robustness === 'degraded').length,
      faulted: input.kube.volumes.filter((v) => v.robustness === 'faulted').length,
      detached: input.kube.volumes.filter((v) => v.state === 'detached').length,
      claims: input.kube.claims.length,
      pendingClaims: input.kube.claims.filter((c) => c.phase !== 'Bound').length,
    },
    infrastructure: {
      switches: input.switches.length,
      kubeNodes: input.kube.nodes.length,
      readyKubeNodes: input.kube.nodes.filter((n) => n.ready).length,
      pressures,
      labVmsRunning: input.truth.vms.filter((v) => v.power === 'Running').length,
      labVms: input.truth.vms.length,
      partitions: input.truth.partitions.length,
      vipHolders: input.truth.vip_holders,
    },
    alerts: {
      active: input.alerts.filter((a) => a.resolved_at === null).length,
      critical: input.alerts.filter((a) => a.resolved_at === null && a.severity === 'Critical').length,
      warning: input.alerts.filter((a) => a.resolved_at === null && a.severity === 'Warning').length,
    },
    attention: deduped.slice(0, 10),
  }
}
