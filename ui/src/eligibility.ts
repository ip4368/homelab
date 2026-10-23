import type { HardwareProfile } from './gen/HardwareProfile'
import type { Role } from './gen/Role'

/** Every role, in the server's declaration order. */
export const ALL_ROLES: Role[] = ['ControlPlane', 'Storage', 'Compute', 'Vm', 'Transcode']

/**
 * Why a host with hardware `p` cannot take role `r`, or null when it can.
 * Mirrors `model::roles::eligible`, with the same reasons.
 */
export function ineligibleReason(p: HardwareProfile, r: Role): string | null {
  switch (r) {
    case 'Compute':
      return null
    case 'Storage':
      return p.disks.some((d) => d.kind !== 'Sd' && d.kind !== 'Emmc') ? null : 'storage needs a non-SD, non-eMMC disk'
    case 'ControlPlane':
      if (p.ram_gb < 8) return `control-plane needs at least 8 GB RAM (has ${p.ram_gb} GB)`
      if (p.arch !== 'X86_64' && p.id !== 'Rk3588') return 'control-plane needs an x86_64 host or an RK3588'
      return null
    case 'Vm':
      return p.ram_gb >= 16 ? null : `vm needs at least 16 GB RAM (has ${p.ram_gb} GB)`
    case 'Transcode':
      return p.capabilities.some((c) => c.startsWith('hw-transcode:')) ? null : 'transcode needs an hw-transcode:* capability'
  }
}

export function eligibleRoles(p: HardwareProfile): Role[] {
  return ALL_ROLES.filter((r) => ineligibleReason(p, r) === null)
}
