import { describe, expect, it } from 'vitest'
import type { HostObj } from './gen/HostObj'
import type { Role } from './gen/Role'
import { groupHosts } from './grouping'

const host = (
  id: string,
  profile: HostObj['profile']['id'],
  roles: Role[] = [],
  location = 'rack-0',
  lldp: HostObj['lldp'] = [],
) => ({ id, profile: { id: profile }, roles, location, lldp }) as HostObj

const neighbor = (local_port: string, sw: string): HostObj['lldp'][number] => ({
  local_port,
  switch: sw,
  switch_port: 'p1',
})

describe('groupHosts', () => {
  it('groups by profile sorted', () => {
    const hosts = [host('h3', 'Pi5'), host('h2', 'Ms01'), host('h1', 'Pi5'), host('h0', 'MiniPc')]
    const groups = groupHosts(hosts, 'profile')
    expect(groups.map((g) => g.key)).toEqual(['MiniPc', 'Ms01', 'Pi5'])
    expect(groups[2].hosts.map((h) => h.id)).toEqual(['h1', 'h3'])
  })

  it('host with multiple roles appears in each role group', () => {
    const multi = host('h1', 'Ms01', ['Storage', 'Vm'])
    const groups = groupHosts([multi, host('h2', 'Pi5', ['Storage'])], 'role')
    expect(groups.map((g) => g.key)).toEqual(['Storage', 'Vm'])
    expect(groups[0].hosts.map((h) => h.id)).toEqual(['h1', 'h2'])
    expect(groups[1].hosts).toEqual([multi])
  })

  it('puts a host with no roles in the "none" group', () => {
    expect(groupHosts([host('h1', 'Pi5')], 'role')).toEqual([{ key: 'none', hosts: [host('h1', 'Pi5')] }])
  })

  it('groups by rack from the host location, naturally ordered', () => {
    const groups = groupHosts([host('a', 'Pi5', [], 'rack-10'), host('b', 'Pi5', [], 'rack-2')], 'rack')
    expect(groups.map((g) => g.key)).toEqual(['rack-2', 'rack-10'])
  })

  it('groups by the switch the mgmt port is cabled to, from LLDP', () => {
    const hosts = [
      host('ms', 'Ms01', [], 'rack-0', [neighbor('sfp0', 'core-1'), neighbor('eth0', 'access-1')]),
      host('pi', 'Pi5', [], 'rack-2', [neighbor('eth0', 'access-2')]),
      host('mini', 'MiniPc', [], 'rack-1', [neighbor('eth0', 'access-1')]),
    ]
    const groups = groupHosts(hosts, 'switch')
    expect(groups.map((g) => [g.key, g.hosts.map((h) => h.id)])).toEqual([
      ['access-1', ['mini', 'ms']],
      ['access-2', ['pi']],
    ])
  })

  it('switch grouping is unknown until a host reports its mgmt neighbour', () => {
    const hosts = [
      host('b', 'Pi5'),
      host('a', 'Ms01', [], 'rack-0', [neighbor('sfp0', 'core-1')]),
      host('c', 'Pi5', [], 'rack-1', [neighbor('eth0', 'access-1')]),
    ]
    const groups = groupHosts(hosts, 'switch')
    expect(groups.map((g) => g.key)).toEqual(['access-1', 'unknown'])
    expect(groups[1].hosts.map((h) => h.id)).toEqual(['a', 'b'])
  })
})
