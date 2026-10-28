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

/**
 * Why role `r` is not set by hand, or null when it is: the control-plane
 * role follows the control-plane voters, and the server refuses changes to it.
 */
export function fixedRoleReason(r: Role): string | null {
  return r === 'ControlPlane' ? 'the control-plane role follows the control-plane voters: hand a voter its place in the Control plane section' : null
}

export function eligibleRoles(p: HardwareProfile): Role[] {
  return ALL_ROLES.filter((r) => ineligibleReason(p, r) === null)
}
