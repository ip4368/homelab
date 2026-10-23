import type { HostObj } from './gen/HostObj'

export type GroupBy = 'rack' | 'switch' | 'role' | 'profile'

export const GROUP_BYS: GroupBy[] = ['rack', 'switch', 'role', 'profile']

export interface HostGroup {
  key: string
  hosts: HostObj[]
}

const byText = (a: string, b: string) => a.localeCompare(b, 'en', { numeric: true })

/** The NIC a host's mgmt network rides on, whose LLDP neighbour is its access switch. */
const MGMT_NIC = 'eth0'

/** The switch the host's mgmt port is cabled to, from its LLDP report; 'unknown' until it reports. */
function mgmtSwitch(h: HostObj): string {
  return h.lldp?.find((n) => n.local_port === MGMT_NIC)?.switch ?? 'unknown'
}

/** The groups a host belongs to; a host with several roles is in each role group. */
function keysOf(h: HostObj, by: GroupBy): string[] {
  switch (by) {
    case 'rack':
      return [h.location]
    case 'profile':
      return [h.profile.id]
    case 'role':
      return h.roles.length > 0 ? h.roles : ['none']
    case 'switch':
      return [mgmtSwitch(h)]
  }
}

/** Groups sorted by key (numbers inside keys sort numerically), hosts within a group by id. */
export function groupHosts(hosts: HostObj[], by: GroupBy): HostGroup[] {
  const groups = new Map<string, HostObj[]>()
  for (const h of hosts) {
    for (const key of keysOf(h, by)) {
      const members = groups.get(key)
      if (members) members.push(h)
      else groups.set(key, [h])
    }
  }
  return [...groups.entries()]
    .sort(([a], [b]) => byText(a, b))
    .map(([key, members]) => ({ key, hosts: members.sort((a, b) => byText(a.id, b.id)) }))
}
