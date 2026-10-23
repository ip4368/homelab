import { describe, expect, it } from 'vitest'
import type { HardwareProfile } from './gen/HardwareProfile'
import { eligibleRoles, ineligibleReason } from './eligibility'

const profile = (over: Partial<HardwareProfile>): HardwareProfile => ({
  id: 'Ms01',
  arch: 'X86_64',
  cpu_threads: 8,
  ram_gb: 32,
  nics: [],
  disks: [{ name: 'nvme0', kind: 'Nvme', size_gb: 512 }],
  capabilities: [],
  ...over,
})

describe('ineligibleReason', () => {
  it('compute is always allowed', () => {
    expect(ineligibleReason(profile({ ram_gb: 1, disks: [] }), 'Compute')).toBeNull()
  })

  it('storage needs a non-SD, non-eMMC disk', () => {
    const sdOnly = profile({ disks: [{ name: 'sd0', kind: 'Sd', size_gb: 64 }, { name: 'e0', kind: 'Emmc', size_gb: 32 }] })
    expect(ineligibleReason(sdOnly, 'Storage')).toMatch(/non-SD/)
    expect(ineligibleReason(profile({ disks: [...sdOnly.disks, { name: 'n', kind: 'Ssd', size_gb: 1 }] }), 'Storage')).toBeNull()
    expect(ineligibleReason(profile({ disks: [] }), 'Storage')).toMatch(/non-SD/)
  })

  it('control-plane needs 8 GB and x86_64 or RK3588', () => {
    expect(ineligibleReason(profile({}), 'ControlPlane')).toBeNull()
    expect(ineligibleReason(profile({ ram_gb: 4 }), 'ControlPlane')).toMatch(/8 GB.*has 4 GB/)
    expect(ineligibleReason(profile({ id: 'Pi5', arch: 'Arm64', ram_gb: 8 }), 'ControlPlane')).toMatch(/x86_64/)
    expect(ineligibleReason(profile({ id: 'Rk3588', arch: 'Arm64', ram_gb: 8 }), 'ControlPlane')).toBeNull()
  })

  it('vm needs 16 GB', () => {
    expect(ineligibleReason(profile({ ram_gb: 16 }), 'Vm')).toBeNull()
    expect(ineligibleReason(profile({ ram_gb: 8 }), 'Vm')).toMatch(/16 GB.*has 8 GB/)
  })

  it('transcode needs an hw-transcode capability', () => {
    expect(ineligibleReason(profile({}), 'Transcode')).toMatch(/hw-transcode/)
    expect(ineligibleReason(profile({ capabilities: ['hw-transcode:vaapi'] }), 'Transcode')).toBeNull()
  })
})

describe('eligibleRoles', () => {
  it('lists the roles the profile can take, in declaration order', () => {
    const pi = profile({ id: 'Pi5', arch: 'Arm64', ram_gb: 8, disks: [{ name: 's', kind: 'Sd', size_gb: 64 }], capabilities: ['hw-transcode:v4l2'] })
    expect(eligibleRoles(pi)).toEqual(['Compute', 'Transcode'])
  })
})
