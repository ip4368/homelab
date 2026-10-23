import type { HostObj } from './gen/HostObj'

export type GroupBy = 'rack' | 'switch' | 'role' | 'profile'

export const GROUP_BYS: GroupBy[] = ['rack', 'switch', 'role', 'profile']

export interface HostGroup {
  key: string
  hosts: HostObj[]
}

const byText = (a: string, b: string) => a.localeCompare(b, 'en', { numeric: true })

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
      // The host-to-switch cabling arrives with LLDP data (Task 13).
      return ['unknown']
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
