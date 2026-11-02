import { describe, expect, it } from 'vitest'
import type { ApplyReport } from './gen/ApplyReport'
import type { KubePod } from './gen/KubePod'
import type { KubeWorkload } from './gen/KubeWorkload'
import type { LonghornVolume } from './gen/LonghornVolume'
import { age, deletePrompt, editDraft, filterKube, outcomeLabel, previewVerdict, gib, podTone, podsToDrain, removeHostPrompt, replicaLabel, restartPrompt, volumeHosts, workloadActions, workloadClaims, workloadHosts, workloadTone, type KubeFilter } from './kube'

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
})

describe('draining', () => {
  const pods = [
    pod('demo', 'web-1', 'a', 'Deployment/web'),
    pod('longhorn-system', 'longhorn-manager-x', 'a', 'DaemonSet/longhorn-manager'),
    pod('kube-system', 'static', 'a', 'Node/a'),
    pod('demo', 'job-1', 'a', 'Job/job', 'Completed'),
    pod('demo', 'postgres-0', 'a', 'StatefulSet/postgres', 'Pending'),
  ]

  it('leaves DaemonSet, static and finished pods, as kubectl drain does', () => {
    expect(podsToDrain(pods).map((p) => p.name)).toEqual(['web-1', 'postgres-0'])
  })

  it('says what removing a host leaves behind', () => {
    expect(removeHostPrompt('a', pods)).toContain('2 pods still run on it: drain it first')
    expect(removeHostPrompt('a', pods.slice(1, 4))).toContain('No pods need moving off it.')
  })

  it('offers no changes in the system namespaces, and no scaling of a DaemonSet', () => {
    expect(workloadActions(web)).toEqual({ scale: true, restart: true, edit: true, remove: true })
    expect(workloadActions({ namespace: 'demo', kind: 'DaemonSet' })).toEqual({ scale: false, restart: true, edit: true, remove: true })
    for (const namespace of ['kube-system', 'longhorn-system', 'kube-public', 'kube-node-lease']) {
      expect(workloadActions({ namespace, kind: 'Deployment' })).toEqual({ scale: false, restart: false, edit: false, remove: false })
    }
  })

  it("deleting a StatefulSet says its claims stay, naming them", () => {
    const postgres: KubeWorkload = { namespace: 'demo', name: 'postgres', kind: 'StatefulSet', desired: 1, ready: 1 }
    const pgPods = [pod('demo', 'postgres-0', 'ms01-04', 'StatefulSet/postgres'), pod('other', 'postgres-0', 'ms01-04', 'StatefulSet/postgres')]
    const elsewhere = { ...volume, name: 'pvc-2', pvc: 'other/data-postgres-0' }
    const unused = { ...volume, name: 'pvc-3', pvc: 'demo/scratch', pods: [] }
    const claims = workloadClaims(postgres, pgPods, [volume, elsewhere, unused])
    expect(claims).toEqual(['demo/data-postgres-0'])
    expect(deletePrompt(postgres, claims)).toBe(
      'Delete StatefulSet demo/postgres and its pods? Its volume claims stay, with their data: demo/data-postgres-0. Delete them with kubectl once you no longer need them.',
    )
    expect(deletePrompt(postgres, [])).toContain('Its volume claims stay.')
    expect(deletePrompt(web, [])).toBe('Delete Deployment demo/web and its pods?')
    expect(restartPrompt(web)).toContain('Restart Deployment demo/web?')
  })

  it('says what became of each applied object, done or to be done', () => {
    expect(outcomeLabel('Created', true)).toEqual({ text: 'will be created', tone: 'ok' })
    expect(outcomeLabel('Created', false)).toEqual({ text: 'created', tone: 'ok' })
    expect(outcomeLabel('Configured', true).text).toBe('will change')
    expect(outcomeLabel('Unchanged', false).tone).toBe('muted')
    expect(outcomeLabel({ Failed: { message: 'spec.replicas: Invalid value: -1' } }, true)).toEqual({ text: 'spec.replicas: Invalid value: -1', tone: 'bad' })
    expect(outcomeLabel({ Conflict: { message: 'x', conflicts: [] } }, true).tone).toBe('warn')
  })

  it('a preview allows applying only if nothing failed or conflicted, and lists what conflicts', () => {
    const report = (outcomes: ApplyReport['objects'][number]['outcome'][]): ApplyReport => ({
      dry_run: true,
      objects: outcomes.map((outcome, n) => ({ kind: 'Deployment', namespace: 'demo', name: `w${n}`, outcome })),
    })
    expect(previewVerdict(report(['Created', 'Unchanged', 'AfterNamespace']))).toEqual({ canApply: true, conflicts: [] })
    expect(previewVerdict(report(['Created', { Failed: { message: 'no' } }, 'Skipped'])).canApply).toBe(false)
    const conflicted = previewVerdict(report([{ Conflict: { message: 'x', conflicts: [{ manager: 'kubectl-client-side-apply', field: '.spec.replicas' }] } }, 'Skipped']))
    expect(conflicted).toEqual({ canApply: false, conflicts: [{ manager: 'kubectl-client-side-apply', field: '.spec.replicas', object: 'Deployment w0' }] })
  })

  it('an edit of a live workload starts by taking over what it changes', () => {
    const d = editDraft(web, 'kind: Deployment')
    expect(d).toMatchObject({ title: 'Edit Deployment demo/web', yaml: 'kind: Deployment', namespace: 'demo', force: true })
  })
})
