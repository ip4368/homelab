import { describe, expect, it } from 'vitest'
import { MISCONFIGS, misconfigOf, partitionOf } from './FaultPanel'

describe('fault panel forms', () => {
  it('maps each misconfiguration choice to the AddHosts scenario shape', () => {
    expect(MISCONFIGS.map((m) => m.value)).toEqual(['none', 'MissingVlan20', 'MtuTooSmall', 'BondNoLacp'])
    expect(misconfigOf('none')).toBeNull()
    expect(misconfigOf('MissingVlan20')).toEqual({ MissingVlan: 20 })
    expect(misconfigOf('MtuTooSmall')).toBe('MtuTooSmall')
    expect(misconfigOf('BondNoLacp')).toBe('BondNoLacp')
    expect(misconfigOf('bogus')).toBeNull()
  })

  it('builds one partition group from a host set and an optional switch', () => {
    expect(partitionOf(' ms01-01, ms01-02 ms01-01 ', '')).toEqual({ Partition: { nodes: [{ Host: 'ms01-01' }, { Host: 'ms01-02' }] } })
    expect(partitionOf('pi5-01', ' access-2 ')).toEqual({ Partition: { nodes: [{ Switch: 'access-2' }, { Host: 'pi5-01' }] } })
    expect(partitionOf('', 'access-2')).toEqual({ Partition: { nodes: [{ Switch: 'access-2' }] } })
    expect(partitionOf(' , ', ' ')).toBeNull()
  })
})
