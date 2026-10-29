import { describe, expect, it } from 'vitest'
import type { LabTruth } from './gen/LabTruth'
import type { Power } from './gen/Power'
import type { HostObj } from './gen/HostObj'
import { truthDiff } from './truth'

const host = (id: string, lifecycle: HostObj['lifecycle']) => ({ id, lifecycle }) as HostObj
const truthOf = (vms: LabTruth['vms']): Pick<LabTruth, 'vms'> => ({ vms })
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
  it('flags powered-off host still Ready', () => {
    const view = { hosts: { a: host('a', 'Ready'), b: host('b', 'Ready') } }
    const diff = truthDiff(view, truthOf([up('a', false), up('b')]))
    expect(diff).toEqual([{ hostId: 'a', believed: 'Ready', actual: 'powered off' }])
  })

  it('flags a frozen host the view still has up', () => {
    const view = { hosts: { a: host('a', 'Ready') } }
    expect(truthDiff(view, truthOf([vm('a', 'Paused')]))).toEqual([{ hostId: 'a', believed: 'Ready', actual: 'frozen' }])
  })

  it('flags a NotReady host that is running (a partition), and treats cordoned hosts as believed up', () => {
    const view = { hosts: { a: host('a', 'NotReady'), b: host('b', 'Ready'), c: host('c', 'Cordoned'), d: host('d', 'NotReady') } }
    const diff = truthDiff(view, truthOf([up('a'), up('b', false), up('c'), up('d', false)]))
    expect(diff).toEqual([
      { hostId: 'a', believed: 'NotReady', actual: 'running' },
      { hostId: 'b', believed: 'Ready', actual: 'powered off' },
    ])
  })

  it('ignores hosts that have not joined, are removed, or are unknown to the truth', () => {
    const view = { hosts: { p: host('p', 'Pending'), r: host('r', 'Removed'), x: host('x', 'Ready') } }
    expect(truthDiff(view, truthOf([up('p', false), up('r', false)]))).toEqual([])
  })
})
