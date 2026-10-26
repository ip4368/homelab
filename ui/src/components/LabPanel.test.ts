import { describe, expect, it } from 'vitest'
import { MISTAKES, mistakeOf, partitionOf } from './LabPanel'

describe('lab panel forms', () => {
  it('maps each technician mistake choice to the AddVm shape', () => {
    expect(MISTAKES.map((m) => m.value)).toEqual(['none', 'MissingStorageVlan', 'MtuTooSmall', 'NoLacp'])
    expect(mistakeOf('none')).toBeNull()
    expect(mistakeOf('MissingStorageVlan')).toBe('MissingStorageVlan')
    expect(mistakeOf('NoLacp')).toBe('NoLacp')
    expect(mistakeOf('bogus')).toBeNull()
  })

  it('builds one partition group from a machine set and an optional switch', () => {
    expect(partitionOf(' ms01-01, ms01-02 ms01-01 ', '')).toEqual({ Partition: { members: [{ Host: 'ms01-01' }, { Host: 'ms01-02' }] } })
    expect(partitionOf('minipc-01', ' access-1 ')).toEqual({ Partition: { members: [{ Switch: 'access-1' }, { Host: 'minipc-01' }] } })
    expect(partitionOf('', 'core-1')).toEqual({ Partition: { members: [{ Switch: 'core-1' }] } })
    expect(partitionOf(' , ', ' ')).toBeNull()
  })
})
