import { describe, expect, it } from 'vitest'
import type { GroundTruth } from './gen/GroundTruth'
import type { HostObj } from './gen/HostObj'
import {
  buildWorkloads,
  filterWorkloads,
  hostWorkloads,
  migrationTargets,
  removeHostPrompt,
  truthDiff,
  validateDeployment,
  validateVm,
  validateVolume,
  type WorkloadFilter,
  type WorkloadSource,
} from './workloads'

const host = (id: string, lifecycle: HostObj['lifecycle'], roles: HostObj['roles'] = []) => ({ id, lifecycle, roles }) as HostObj
const truthOf = (over: Partial<GroundTruth> = {}): Pick<GroundTruth, 'hosts' | 'split_brain_vms'> => ({ hosts: [], split_brain_vms: [], ...over })
const up = (id: string, powered = true) => ({ id, powered, cp_role: null })

function source(): WorkloadSource {
  return {
    deployments: { web: { version: 1, id: 'web', image: 'nginx', replicas: 2, cpu_m: 100, ram_mb: 64, selector: { roles: [], capability_prefix: null } } },
    pods: {
      'web/0': { version: 1, deployment: 'web', idx: 0, host: 'a', phase: 'Running' },
      'web/1': { version: 1, deployment: 'web', idx: 1, host: null, phase: { Pending: 'unscheduled' } },
    },
    vms: {
      vm1: { version: 1, id: 'vm1', vcpu: 2, ram_mb: 2048, disk_gb: 20, networks: ['vm-lan'], volume: 'vol1' },
      vm2: { version: 1, id: 'vm2', vcpu: 2, ram_mb: 2048, disk_gb: 20, networks: [], volume: 'vol2' },
    },
    vmStatus: {
      vm1: { id: 'vm1', version: 1, host: 'a', phase: 'Running', since: 0 },
      vm2: { id: 'vm2', version: 1, host: 'a', phase: { Migrating: { to: 'b' } }, since: 0 },
    },
    volumes: {
      vol1: { version: 1, id: 'vol1', size_gb: 20, replicas: 3 },
      vol2: { version: 1, id: 'vol2', size_gb: 20, replicas: 2 },
    },
    volumeStatus: {
      vol1: { id: 'vol1', version: 1, health: 'Healthy', replicas: [{ host: 'a', disk: 'nvme0', state: 'Healthy' }, { host: 'b', disk: 'nvme0', state: 'Healthy' }] },
      vol2: { id: 'vol2', version: 1, health: 'Rebuilding', replicas: [{ host: 'b', disk: 'nvme0', state: { Rebuilding: { pct: 40 } } }, { host: 'c', disk: 'nvme0', state: 'Stale' }] },
    },
  }
}

const all: WorkloadFilter = { kind: 'all', text: '', host: '', health: '' }

describe('buildWorkloads', () => {
  it('lists VMs, pods and volumes with their hosts and health', () => {
    const items = buildWorkloads(source())
    expect(items.map((i) => `${i.kind}:${i.id}`)).toEqual(['vm:vm1', 'vm:vm2', 'pod:web/0', 'pod:web/1', 'volume:vol1', 'volume:vol2'])
    const byId = Object.fromEntries(items.map((i) => [i.id, i]))
    expect(byId.vm1).toMatchObject({ hosts: ['a'], health: 'Running' })
    expect(byId.vm2).toMatchObject({ hosts: ['a', 'b'], health: 'Migrating' })
    expect(byId['web/1']).toMatchObject({ hosts: [], health: 'Pending' })
    expect(byId['web/1'].detail).toContain('unscheduled')
    expect(byId.vol2).toMatchObject({ hosts: ['b', 'c'], health: 'Rebuilding' })
  })

  it('shows a VM without a status as Pending', () => {
    const src = source()
    src.vmStatus = {}
    expect(buildWorkloads(src).find((i) => i.id === 'vm1')).toMatchObject({ hosts: [], health: 'Pending' })
  })
})

describe('filterWorkloads', () => {
  const items = buildWorkloads(source())

  it('filter by kind and health', () => {
    expect(filterWorkloads(items, { ...all, kind: 'volume' }).map((i) => i.id)).toEqual(['vol1', 'vol2'])
    expect(filterWorkloads(items, { ...all, kind: 'volume', health: 'Rebuilding' }).map((i) => i.id)).toEqual(['vol2'])
    expect(filterWorkloads(items, { ...all, kind: 'pod', health: 'Rebuilding' })).toEqual([])
    expect(filterWorkloads(items, { ...all, health: 'Running' }).map((i) => i.id)).toEqual(['vm1', 'web/0'])
  })

  it('filters by host and by text on the id, case-insensitively', () => {
    expect(filterWorkloads(items, { ...all, host: 'b' }).map((i) => i.id)).toEqual(['vm2', 'vol1', 'vol2'])
    expect(filterWorkloads(items, { ...all, text: 'WEB' }).map((i) => i.id)).toEqual(['web/0', 'web/1'])
    expect(filterWorkloads(items, all)).toHaveLength(6)
  })
})

describe('truthDiff', () => {
  it('truthDiff flags powered-off host still Ready', () => {
    const view = { hosts: { a: host('a', 'Ready'), b: host('b', 'Ready') } }
    const diff = truthDiff(view, truthOf({ hosts: [up('a', false), up('b')] }))
    expect(diff).toEqual([{ hostId: 'a', believed: 'Ready', actual: 'powered off' }])
  })

  it('flags a NotReady host that is powered (a partition), and treats cordoned/draining hosts as believed up', () => {
    const view = { hosts: { a: host('a', 'NotReady'), b: host('b', 'Draining'), c: host('c', 'Cordoned'), d: host('d', 'NotReady') } }
    const diff = truthDiff(view, truthOf({ hosts: [up('a'), up('b', false), up('c'), up('d', false)] }))
    expect(diff).toEqual([
      { hostId: 'a', believed: 'NotReady', actual: 'powered on' },
      { hostId: 'b', believed: 'Draining', actual: 'powered off' },
    ])
  })

  it('ignores hosts that have not joined, are removed, or are unknown to the truth', () => {
    const view = { hosts: { p: host('p', 'Pending'), r: host('r', 'Removed'), x: host('x', 'Ready') } }
    expect(truthDiff(view, truthOf({ hosts: [up('p', false), up('r', false)] }))).toEqual([])
  })

  it('truthDiff lists split-brain vm', () => {
    const diff = truthDiff({ hosts: {} }, truthOf({ split_brain_vms: ['vm1'] }))
    expect(diff).toEqual([{ hostId: 'vm:vm1', believed: 'runs on one host', actual: 'split brain: running on several hosts' }])
  })
})

describe('hostWorkloads', () => {
  it('lists what the view places on a host: VMs (also incoming), pods and replicas', () => {
    const src = source()
    expect(hostWorkloads(src, 'a')).toEqual({ vms: ['vm1', 'vm2'], pods: ['web/0'], replicas: [{ volume: 'vol1', disk: 'nvme0', state: 'Healthy' }] })
    expect(hostWorkloads(src, 'b').vms).toEqual(['vm2'])
    expect(hostWorkloads(src, 'b').replicas.map((r) => r.volume)).toEqual(['vol1', 'vol2'])
    expect(hostWorkloads(src, 'zzz')).toEqual({ vms: [], pods: [], replicas: [] })
  })
})

describe('removeHostPrompt', () => {
  it('names what runs on the host and what removal does to it', () => {
    const w = { vms: ['a', 'b'], pods: ['web/0'], replicas: [{ volume: 'v', disk: 'nvme0', state: 'Healthy' as const }] }
    const text = removeHostPrompt('ms01-04', w)
    expect(text).toContain('Remove ms01-04?')
    expect(text).toContain('2 VMs, 1 pod and 1 volume replica')
    expect(text).toContain('cannot rejoin')
    expect(removeHostPrompt('pi5-01', { vms: [], pods: [], replicas: [] })).toContain('Nothing is placed on it')
  })
})

describe('migrationTargets', () => {
  it('offers Ready hosts with the Vm role, except the one the VM is on', () => {
    const hosts = {
      a: host('a', 'Ready', ['Vm']),
      b: host('b', 'Ready', ['Vm', 'Compute']),
      c: host('c', 'Cordoned', ['Vm']),
      d: host('d', 'Ready', ['Compute']),
      e: host('e', 'NotReady', ['Vm']),
    }
    expect(migrationTargets(hosts, 'a')).toEqual(['b'])
    expect(migrationTargets(hosts, null)).toEqual(['a', 'b'])
  })
})

describe('validation', () => {
  const dep = { id: 'web', image: 'nginx', replicas: 3, cpu_m: 100, ram_mb: 64, selector: { roles: [], capability_prefix: null } }

  it('deployments need 1 to 1000 replicas, a name, an image and resources', () => {
    expect(validateDeployment(dep, [])).toBeNull()
    expect(validateDeployment({ ...dep, replicas: 0 }, [])).toBe('replicas must be 1 to 1000, not 0')
    expect(validateDeployment({ ...dep, replicas: 1000 }, [])).toBeNull()
    expect(validateDeployment({ ...dep, replicas: 1001 }, [])).toBe('replicas must be 1 to 1000, not 1001')
    expect(validateDeployment({ ...dep, id: ' ' }, [])).toMatch(/name/)
    expect(validateDeployment({ ...dep, image: '' }, [])).toMatch(/image/)
    expect(validateDeployment({ ...dep, cpu_m: 0 }, [])).toMatch(/cpu/)
    expect(validateDeployment(dep, ['web'])).toMatch(/already exists/)
  })

  it('volumes need at least 1 GB and 1 to 5 replicas', () => {
    const vol = { id: 'v', size_gb: 10, replicas: 3 }
    expect(validateVolume(vol, [])).toBeNull()
    expect(validateVolume({ ...vol, size_gb: 0 }, [])).toBe('size_gb must be at least 1')
    expect(validateVolume({ ...vol, replicas: 0 }, [])).toBe('replicas must be 1 to 5, not 0')
    expect(validateVolume({ ...vol, replicas: 6 }, [])).toBe('replicas must be 1 to 5, not 6')
    expect(validateVolume({ ...vol, replicas: 5 }, [])).toBeNull()
    expect(validateVolume(vol, ['v'])).toMatch(/already exists/)
  })

  it('VMs need sizes and an existing volume', () => {
    const vm = { id: 'vm', vcpu: 2, ram_mb: 1024, disk_gb: 10, networks: ['vm-lan'], volume: 'vol1' }
    expect(validateVm(vm, [], ['vol1'])).toBeNull()
    expect(validateVm({ ...vm, vcpu: 0 }, [], ['vol1'])).toMatch(/vcpu/)
    expect(validateVm({ ...vm, ram_mb: 0 }, [], ['vol1'])).toMatch(/ram/)
    expect(validateVm({ ...vm, disk_gb: 0 }, [], ['vol1'])).toMatch(/disk/)
    expect(validateVm(vm, [], [])).toMatch(/volume/)
    expect(validateVm(vm, ['vm'], ['vol1'])).toMatch(/already exists/)
  })
})
