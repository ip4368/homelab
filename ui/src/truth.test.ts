import { describe, expect, it } from 'vitest'
import type { HostObj } from './gen/HostObj'
import type { LabTruth } from './gen/LabTruth'
import type { Power } from './gen/Power'
import { truthDiff } from './truth'

const host = (id: string, lifecycle: HostObj['lifecycle']) => ({ id, lifecycle, roles: [] }) as unknown as HostObj
const vm = (id: string, power: Power): LabTruth['vms'][number] => ({
  id,
  profile: 'Ms01',
  installed: true,
  power,
  unplugged: [],
  disks: ['nvme0'],
  failed_disks: [],
  holds_vip: false,
  clock_offset_ms: null,
  uptime_s: null,
  installer: null,
})
const up = (id: string, powered = true) => vm(id, powered ? 'Running' : 'Off')

describe('truthDiff', () => {
  it('flags a powered-off host still Ready', () => {
    const view = { hosts: { a: host('a', 'Ready'), b: host('b', 'Ready') } }
    expect(truthDiff(view, { vms: [up('a', false), up('b')] })).toEqual([{ hostId: 'a', believed: 'Ready', actual: 'powered off' }])
  })

  it('flags a frozen host the view still has up', () => {
    const view = { hosts: { a: host('a', 'Ready') } }
    expect(truthDiff(view, { vms: [vm('a', 'Paused')] })).toEqual([{ hostId: 'a', believed: 'Ready', actual: 'frozen' }])
  })

  it('flags a NotReady host that is running (a partition), and treats cordoned/draining hosts as believed up', () => {
    const view = { hosts: { a: host('a', 'NotReady'), b: host('b', 'Draining'), c: host('c', 'Cordoned'), d: host('d', 'NotReady') } }
    expect(truthDiff(view, { vms: [up('a'), up('b', false), up('c'), up('d', false)] })).toEqual([
      { hostId: 'a', believed: 'NotReady', actual: 'running' },
      { hostId: 'b', believed: 'Draining', actual: 'powered off' },
    ])
  })

  it('ignores hosts that have not joined, are removed, or are unknown to the truth', () => {
    const view = { hosts: { p: host('p', 'Pending'), r: host('r', 'Removed'), x: host('x', 'Ready') } }
    expect(truthDiff(view, { vms: [up('p', false), up('r', false)] })).toEqual([])
  })
})
