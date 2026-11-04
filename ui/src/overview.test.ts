import { describe, expect, it } from 'vitest'
import type { Alert } from './gen/Alert'
import type { HostObj } from './gen/HostObj'
import type { KubePod } from './gen/KubePod'
import type { KubeView } from './gen/KubeView'
import type { KubeWorkload } from './gen/KubeWorkload'
import type { LabTruth } from './gen/LabTruth'
import type { MetricSample } from './gen/MetricSample'
import { summarizeOverview, type OverviewInput } from './overview'
import { emptyKube } from './store'

const host = (id: string, lifecycle: HostObj['lifecycle']): HostObj =>
  ({ id, lifecycle, roles: ['Compute'], location: 'rack-1' }) as unknown as HostObj

const metric = (cpu_pct: number, ram_pct: number): MetricSample => ({ at: 1, cpu_pct, ram_pct, temp_c: null, disk_pct: {}, port_rx_bps: {}, port_tx_bps: {} })

const workload = (name: string, state: KubeWorkload['health']['state'], cpu_milli: number | null = null): KubeWorkload =>
  ({
    namespace: 'demo',
    name,
    kind: 'Deployment',
    desired: 2,
    ready: state === 'Healthy' ? 2 : 1,
    updated: 2,
    health: { state, reason: state === 'Down' ? 'CrashLoopBackOff' : null },
    cpu_milli,
    memory_bytes: 1024,
    cpu_limit_milli: null,
    memory_limit_bytes: null,
    cpu_request_milli: null,
    memory_request_bytes: null,
    net_rx_bps: 100,
    net_tx_bps: 50,
    images: [],
    endpoints: [],
    conditions: [],
  }) as KubeWorkload

const pod = (name: string, ready: number, containers: number, restarts: number): KubePod =>
  ({ namespace: 'demo', name, owner: 'Deployment/web', ready, containers, restarts }) as unknown as KubePod

const truth: LabTruth = { vms: [], switches: [], ports: [], partitions: [], vip_holders: [], at: 0 }

function input(patch: Partial<OverviewInput> = {}): OverviewInput {
  return { hosts: [], metrics: {}, alerts: [], switches: [], kube: emptyKube, truth, ...patch }
}

describe('summarizeOverview', () => {
  it('aggregates host and workload health, load, and traffic', () => {
    const kube: KubeView = {
      ...emptyKube,
      workloads: [workload('web', 'Healthy', 300), workload('api', 'Degraded', 100), workload('old', 'Idle', null)],
      pods: [pod('web-1', 1, 1, 0), pod('web-2', 0, 2, 3), pod('api-1', 2, 2, 0)],
      nodes: [{ name: 'ms01-01', ready: true, cpu_allocatable_milli: 8000, memory_allocatable_bytes: 16, pressures: [], kubelet_version: 'v1' }],
    }
    const s = summarizeOverview(input({
      hosts: [host('a', 'Ready'), host('b', 'Ready'), host('c', 'NotReady'), host('d', 'Pending'), host('e', 'Cordoned')],
      metrics: { a: metric(20, 40), b: metric(80, 60), c: metric(90, 10) },
      kube,
    }))
    expect(s.hosts).toMatchObject({ total: 5, ready: 2, notReady: 1, pending: 1, other: 1, sampled: 3, avgCpu: 63.333333333333336, avgRam: 36.666666666666664 })
    expect(s.hosts.busiest.map((h) => h.id)).toEqual(['c', 'b', 'a', 'd', 'e'])
    expect(s.workloads).toMatchObject({ total: 3, healthy: 1, degraded: 1, idle: 1, pods: 3, readyPods: 2, restartingPods: 1, cpuMilli: 400, memoryBytes: 3072, netRxBps: 300, netTxBps: 150 })
    expect(s.workloads.attentionFirst.map((w) => w.name)).toEqual(['api', 'old', 'web'])
  })

  it('builds one deduplicated attention list, failures first', () => {
    const alert: Alert = {
      id: 'a1', severity: 'Warning', kind: 'DiskPressure', entity: { kind: 'Host', id: 'b' }, message: 'disk is nearly full',
      raised_at: 2, resolved_at: null, version: 1,
    }
    const kube: KubeView = {
      ...emptyKube,
      workloads: [workload('web', 'Down')],
      nodes: [{ name: 'a', ready: true, cpu_allocatable_milli: null, memory_allocatable_bytes: null, pressures: ['DiskPressure'], kubelet_version: null }],
      volumes: [{ name: 'pvc-1', pvc: 'demo/data', pods: [], size_bytes: 10, replicas_wanted: 2, state: 'attached', robustness: 'faulted', node: 'a', replicas: [] }],
    }
    const s = summarizeOverview(input({
      hosts: [host('a', 'NotReady'), host('b', 'Ready')],
      alerts: [alert, { ...alert, id: 'resolved', resolved_at: 3 }],
      kube,
      truth: { ...truth, vms: [{ id: 'a', power: 'Running', holds_vip: true }, { id: 'b', power: 'Running', holds_vip: true }] as LabTruth['vms'], vip_holders: ['a', 'b'] },
    }))
    expect(s.attention.map((a) => [a.severity, a.title])).toEqual([
      ['bad', 'a is NotReady'],
      ['bad', 'demo/data is faulted'],
      ['bad', 'demo/web is down'],
      ['bad', 'VIP has multiple holders'],
      ['warn', 'a has DiskPressure'],
      ['warn', 'DiskPressure · b'],
    ])
    expect(s.attention.find((a) => a.title.startsWith('DiskPressure'))?.target).toEqual({ kind: 'host', id: 'b' })
    expect(s.attention.find((a) => a.title.startsWith('demo/web'))?.target).toEqual({ kind: 'workload', key: 'demo/Deployment/web' })
    expect(s.alerts).toEqual({ active: 1, critical: 0, warning: 1 })
    expect(s.infrastructure.labVmsRunning).toBe(2)
  })

  it('stays empty but coherent before any data', () => {
    const s = summarizeOverview(input())
    expect(s.hosts.avgCpu).toBeNull()
    expect(s.hosts.busiest).toEqual([])
    expect(s.workloads.attentionFirst).toEqual([])
    expect(s.attention).toEqual([])
  })
})
