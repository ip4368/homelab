import { ALL_ROLES, fixedRoleReason, ineligibleReason } from '../eligibility'
import type { HardwareProfile } from '../gen/HardwareProfile'
import type { Role } from '../gen/Role'

interface Props {
  profile: HardwareProfile
  value: Role[]
  onChange(roles: Role[]): void
  /** Show only the roles that can be set here, instead of disabling the others. */
  eligibleOnly?: boolean
}

/**
 * Role checkboxes; roles the hardware cannot take, and the control-plane
 * role (which follows the voters), are disabled with the reason as a tooltip.
 */
export function RolePicker({ profile, value, onChange, eligibleOnly }: Props) {
  const toggle = (role: Role, on: boolean) =>
    onChange(ALL_ROLES.filter((r) => (r === role ? on : value.includes(r))))
  return (
    <span className="role-picker">
      {ALL_ROLES.map((role) => {
        const reason = ineligibleReason(profile, role) ?? fixedRoleReason(role)
        if (eligibleOnly && reason) return null
        return (
          <label key={role} className={reason ? 'disabled' : ''} title={reason ?? undefined}>
            <input
              type="checkbox"
              checked={value.includes(role)}
              disabled={reason !== null}
              onChange={(e) => toggle(role, e.target.checked)}
            />
            {role}
          </label>
        )
      })}
    </span>
  )
}
