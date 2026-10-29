import { describe, expect, it } from 'vitest'
import type { HostObj } from './gen/HostObj'
import type { KubePod } from './gen/KubePod'
import type { KubeWorkload } from './gen/KubeWorkload'
import type { LonghornVolume } from './gen/LonghornVolume'
import { age, filterKube, gib, podTone, removalStatus, replicaLabel, volumeHosts, workloadHosts, workloadTone, type KubeFilter } from './kube'

function pod(namespace: string, name: string, node: string | null, owner: string | null, status = 'Running'): KubePod {
  return { namespace, name, owner, node, status, ready: 1, containers: 1, restarts: 0, created: 0 }
}

const web: KubeWorkload = { namespace: 'demo', name: 'web', kind: 'Deployment', desired: 3, ready: 3 }
const manager: KubeWorkload = { namespace: 'longhorn-system', name: 'longhorn-manager', kind: 'DaemonSet', desired: 2, ready: 2 }
const pods = [
  pod('demo', 'web-a', 'ms01-02', 'Deployment/web'),
  pod('demo', 'web-b', 'ms01-01', 'Deployment/web'),
  pod('demo', 'web-c', null, 'Deployment/web', 'Pending'),
  pod('other', 'web-x', 'ms01-03', 'Deployment/web'),
  pod('longhorn-system', 'longhorn-manager-1', 'ms01-03', 'DaemonSet/longhorn-manager'),
]
const volume: LonghornVolume = {
  name: 'pvc-1',
  pvc: 'demo/data-postgres-0',
  pods: ['postgres-0'],
  size_bytes: 2 ** 31,
  replicas_wanted: 3,
  state: 'attached',
  robustness: 'healthy',
  node: 'ms01-04',
  replicas: [
    { name: 'r1', node: 'ms01-02', disk: '/mnt/nvme0', state: 'running', mode: 'RW', failed: false },
    { name: 'r2', node: 'ms01-04', disk: '/mnt/nvme0', state: 'running', mode: 'WO', failed: false },
  ],
}
const all: KubeFilter = { namespace: '', hideSystem: false, host: '', text: '' }

describe('kube', () => {
  it("a workload's hosts are its own pods' nodes", () => {
    expect(workloadHosts(web, pods)).toEqual(['ms01-01', 'ms01-02'])
    expect(volumeHosts(volume)).toEqual(['ms01-02', 'ms01-04'])
  })

  it('filters by namespace, system namespaces, host and name', () => {
    const k = { workloads: [web, manager], pods, volumes: [volume] }
    expect(filterKube(k, all).pods).toHaveLength(5)
    const apps = filterKube(k, { ...all, hideSystem: true })
    expect(apps.workloads.map((w) => w.workload.name)).toEqual(['web'])
    expect(apps.pods.map((p) => p.name)).toEqual(['web-a', 'web-b', 'web-c', 'web-x'])
    expect(filterKube(k, { ...all, hideSystem: true, namespace: 'longhorn-system' }).pods.map((p) => p.name)).toEqual(['longhorn-manager-1'])
    const onHost = filterKube(k, { ...all, host: 'ms01-04' })
    expect([onHost.workloads.length, onHost.pods.length, onHost.volumes.length]).toEqual([0, 0, 1])
    expect(filterKube(k, { ...all, text: 'postgres' }).volumes).toHaveLength(1)
    expect(filterKube(k, { ...all, text: 'deployment/web' }).pods).toHaveLength(4)
  })

  it('colours pods, workloads and replicas by how they are doing', () => {
    expect(podTone({ status: 'Running', ready: 1, containers: 1 })).toBe('ok')
    expect(podTone({ status: 'Running', ready: 0, containers: 1 })).toBe('warn')
    expect(podTone({ status: 'ContainerCreating', ready: 0, containers: 1 })).toBe('warn')
    expect(podTone({ status: 'CrashLoopBackOff', ready: 0, containers: 1 })).toBe('bad')
    expect(podTone({ status: 'Succeeded', ready: 0, containers: 1 })).toBe('muted')
    expect([workloadTone({ ready: 3, desired: 3 }), workloadTone({ ready: 1, desired: 3 }), workloadTone({ ready: 0, desired: 1 })]).toEqual(['ok', 'warn', 'bad'])
    expect(volume.replicas.map((r) => replicaLabel(r).text)).toEqual(['in sync', 'rebuilding'])
    expect(replicaLabel({ ...volume.replicas[0], failed: true }).tone).toBe('bad')
  })

  it('formats ages and sizes like kubectl', () => {
    expect([age(0, 45_000), age(0, 12 * 60_000), age(0, 3 * 3_600_000), age(0, 3 * 86_400_000), age(null, 1)]).toEqual(['45s', '12m', '3h', '3d', ''])
    expect([gib(2 ** 31), gib(1.5 * 2 ** 30)]).toEqual(['2 GiB', '1.5 GiB'])
  })

  it('walks a removal through drain, evict, node deletion and wipe', () => {
    const kube = (over: object) => ({ role: 'Agent', phase: 'Running', node_ready: true, labels: {}, storage_disks: [], version: 'v', detail: null, etcd_members: [], etcd_unread: null, etcd_unstarted: [], storage_off: [], node_cordoned: false, evicted: [], nodes_deleted: [], ...over })
    const host = (id: string, lifecycle: HostObj['lifecycle'], k: object | null) => ({ id, lifecycle, kube: k && kube(k) }) as HostObj
    const exec = (evicted: string[], nodes_deleted: string[]) => host('ms01-01', 'Ready', { role: 'Server', evicted, nodes_deleted })
    expect(removalStatus({ a: host('a', 'Ready', {}) }, 'a')).toBeNull()
    expect(removalStatus({ a: host('a', 'Removing', {}) }, 'a')).toBe('Draining Kubernetes workloads…')
    expect(removalStatus({ a: host('a', 'Removing', { node_cordoned: true }) }, 'a')).toBe('Drained. Evicting Longhorn replicas…')
    const mid = { a: host('a', 'Removing', { node_cordoned: true }), 'ms01-01': exec(['a'], []) }
    expect(removalStatus(mid, 'a')).toBe('Replicas evicted. Deleting the node…')
    const deleted = { a: host('a', 'Removing', {}), 'ms01-01': exec(['a'], ['a']) }
    expect(removalStatus(deleted, 'a')).toBe('Node deleted. Wiping the machine…')
    expect(removalStatus({ a: host('a', 'Removing', null) }, 'a')).toBe('Wiping the machine…')
    expect(removalStatus({ a: host('a', 'Removing', { phase: 'Wiped' }) }, 'a')).toBe('Wiped. Forgetting…')
  })
})
