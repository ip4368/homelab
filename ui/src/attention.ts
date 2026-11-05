/**
 * What needs someone's attention across the cluster, worst first: open
 * alerts, hosts down or waiting to join, workloads not healthy and volumes
 * not healthy. The overview lists it, and the side navigation counts it.
 */
import type { Alert } from './gen/Alert'
import type { HostObj } from './gen/HostObj'
import type { KubeView } from './gen/KubeView'
import { HEALTH_LABELS, volumeTone, workloadKey } from './kube'
import type { Open } from './route'

export interface Attention {
  key: string
  tone: 'bad' | 'warn'
  /** Short: what it is about. */
  title: string
  detail: string
  open: Open | null
  /** An open alert, for its suggested action. */
  alert?: Alert
}

const RANK = { bad: 0, warn: 1 }

export function attention(hosts: Record<string, HostObj>, alerts: Record<string, Alert>, kube: Pick<KubeView, 'workloads' | 'volumes'>): Attention[] {
  const items: Attention[] = []
  for (const a of Object.values(alerts)) {
    if (a.resolved_at !== null) continue
    if (a.severity === 'Info') continue
    items.push({
      key: `alert:${a.id}`,
      tone: a.severity === 'Critical' ? 'bad' : 'warn',
      title: `${a.kind} · ${a.entity.id}`,
      detail: a.message,
      open: a.entity.kind === 'Host' ? { kind: 'host', id: a.entity.id } : a.entity.kind === 'Switch' ? { kind: 'switch', id: a.entity.id } : null,
      alert: a,
    })
  }
  const alerted = new Set(items.flatMap((i) => (i.open?.kind === 'host' ? [i.open.id] : [])))
  for (const h of Object.values(hosts)) {
    if (h.lifecycle === 'NotReady' && !alerted.has(h.id)) items.push({ key: `host:${h.id}`, tone: 'bad', title: h.id, detail: 'Host NotReady: its agent stopped reporting.', open: { kind: 'host', id: h.id } })
    if (h.lifecycle === 'Pending') items.push({ key: `join:${h.id}`, tone: 'warn', title: h.id, detail: `Waiting to join (${h.profile.id}): approve it with roles.`, open: { kind: 'host', id: h.id } })
  }
  for (const w of kube.workloads) {
    const s = w.health.state
    if (s === 'Healthy' || s === 'Idle') continue
    items.push({
      key: `workload:${workloadKey(w)}`,
      tone: s === 'Down' ? 'bad' : 'warn',
      title: `${w.namespace}/${w.name}`,
      detail: `${w.kind} ${HEALTH_LABELS[s]}${w.health.reason ? `: ${w.health.reason}` : ''} (${w.ready}/${w.desired} ready)`,
      open: { kind: 'workload', id: workloadKey(w) },
    })
  }
  for (const v of kube.volumes) {
    const tone = volumeTone(v)
    if (tone === 'ok' || tone === 'muted') continue
    items.push({
      key: `volume:${v.name}`,
      tone,
      title: v.pvc ?? v.name,
      detail: `Longhorn volume ${v.robustness === 'unknown' ? v.state : v.robustness}: ${v.replicas.filter((r) => r.mode === 'RW').length}/${v.replicas_wanted} replicas in sync.`,
      open: null,
    })
  }
  return items.sort((a, b) => RANK[a.tone] - RANK[b.tone])
}
