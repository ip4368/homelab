import { describe, expect, it } from 'vitest'
import type { Alert } from './gen/Alert'
import type { HostObj } from './gen/HostObj'
import type { Issue } from './gen/Issue'
import type { PortLoad } from './gen/PortLoad'
import type { SwitchObj } from './gen/SwitchObj'
import { layoutTopology, suggestedFixes, utilisationBand } from './topology'

const cfg = { mode: { Access: 1 }, mtu: 1500, lacp_group: null, admin_up: true } as const

function sw(id: string, ports: string[] = [], mlag_peer: string | null = null): SwitchObj {
  return {
    id,
    model: 'm',
    mlag_peer,
    last_seen: 0,
    version: 1,
    ports: ports.map((port) => ({ switch: id, port, cfg, mlag_peer })),
  }
}

/** A host with an eth0 into `access` port `p<n>` and optional extra legs `[nic, switch, port]`. */
function host(id: string, access: string, n: number, extra: [string, string, string][] = [], issues: Issue[] = []): HostObj {
  const legs: [string, string, string][] = [['eth0', access, `p${n}`], ...extra]
  return {
    id,
    lldp: legs.map(([local_port, s, switch_port]) => ({ local_port, switch: s, switch_port })),
    inventory: { nics: legs.map(([name]) => ({ name, speed_mbps: 2500, link_up: true })) },
    net: { issues, desired: null, applied: null },
  } as unknown as HostObj
}

const cores = [sw('core-1', [], 'core-2'), sw('core-2', [], 'core-1')]

describe('layoutTopology', () => {
  it('cores above access', () => {
    const switches = [sw('access-1'), ...cores, sw('access-2')]
    const hosts = [host('h1', 'access-1', 1), host('h2', 'access-2', 1)]
    const { nodes } = layoutTopology(switches, hosts, { collapseOver: 24 })
    const y = (id: string) => nodes.find((n) => n.id === id)!.y
    expect(y('core-1')).toBe(y('core-2'))
    expect(y('access-1')).toBe(y('access-2'))
    expect(y('core-1')).toBeLessThan(y('access-1'))
    expect(y('access-1')).toBeLessThan(y('h1'))
    expect(nodes.find((n) => n.id === 'core-1')!.kind).toBe('core')
    expect(nodes.find((n) => n.id === 'access-1')!.kind).toBe('access')
  })

  it('puts hosts under the access switch their mgmt port plugs into and draws MS-01 legs to the cores', () => {
    const ms01 = host('ms01-01', 'access-1', 1, [
      ['sfp0', 'core-1', 'p1'],
      ['sfp1', 'core-2', 'p1'],
    ])
    const { nodes, edges } = layoutTopology([sw('access-1'), ...cores], [ms01, host('pi5-01', 'access-1', 2)], { collapseOver: 24 })
    const x = (id: string) => nodes.find((n) => n.id === id)!.x
    expect(Math.abs(x('ms01-01') - x('access-1'))).toBeLessThan(200)
    const legs = edges.filter((e) => e.a === 'ms01-01').map((e) => e.b).sort()
    expect(legs).toEqual(['access-1', 'core-1', 'core-2'])
    // Each access switch is dual-homed to every core.
    expect(edges.filter((e) => e.a === 'access-1' && e.b.startsWith('core-')).map((e) => e.b)).toEqual(['core-1', 'core-2'])
  })

  it('collapses large access switch into group', () => {
    const hosts = Array.from({ length: 30 }, (_, i) => host(`h${i}`, 'access-1', i + 1))
    hosts.push(host('small', 'access-2', 1))
    const { nodes, edges } = layoutTopology([sw('access-1'), sw('access-2'), ...cores], hosts, { collapseOver: 24 })
    const group = nodes.find((n) => n.kind === 'group')!
    expect(group).toMatchObject({ id: 'group:access-1', hosts: 30 })
    expect(nodes.filter((n) => n.kind === 'host').map((n) => n.id)).toEqual(['small'])
    expect(edges.some((e) => e.a === 'group:access-1' && e.b === 'access-1')).toBe(true)
    expect(edges.every((e) => nodes.some((n) => n.id === e.a) && nodes.some((n) => n.id === e.b))).toBe(true)

    // Exactly at the limit stays expanded.
    const at = layoutTopology([sw('access-1'), ...cores], hosts.slice(0, 24), { collapseOver: 24 })
    expect(at.nodes.some((n) => n.kind === 'group')).toBe(false)
  })

  it('mismatch edge flagged when host issues reference that switch port', () => {
    const issue: Issue = { VlanNotAllowed: { network: 'storage', host_port: 'eth0', switch: 'access-1', switch_port: 'p1', vlan: 20 } }
    const other: Issue = { VlanNotAllowed: { network: 'storage', host_port: 'eth0', switch: 'access-2', switch_port: 'p1', vlan: 20 } }
    const hosts = [host('bad', 'access-1', 1, [], [issue]), host('good', 'access-1', 2, [], [other]), host('fine', 'access-1', 3)]
    const { edges } = layoutTopology([sw('access-1'), sw('access-2'), ...cores], hosts, { collapseOver: 24 })
    const state = (a: string) => edges.find((e) => e.a === a && e.b === 'access-1')!.state
    expect(state('bad')).toBe('mismatch')
    expect(state('good')).toBe('up')
    expect(state('fine')).toBe('up')
  })

  it('flags a LACP mismatch on the named switch port and a split bond on its member legs', () => {
    const lacp: Issue = { LacpNotConfigured: { link: 'bond0', switch: 'core-1', port: 'p1', networks: ['storage'] } }
    const split: Issue = { BondSplitWithoutMlag: { link: 'bond0', networks: ['storage'] } }
    const mk = (issue: Issue) => {
      const h = host('ms01-01', 'access-1', 1, [['sfp0', 'core-1', 'p1'], ['sfp1', 'core-2', 'p1']], [issue])
      h.net.desired = {
        links: [{ name: 'bond0', kind: { Bond: { members: ['sfp0', 'sfp1'], mode: 'Lacp' } }, mtu: 9000 }],
        attachments: [],
      } as never
      return layoutTopology([sw('access-1'), ...cores], [h], { collapseOver: 24 }).edges
    }
    const states = (edges: ReturnType<typeof mk>) => Object.fromEntries(edges.filter((e) => e.a === 'ms01-01').map((e) => [e.b, e.state]))
    expect(states(mk(lacp))).toEqual({ 'access-1': 'up', 'core-1': 'mismatch', 'core-2': 'up' })
    expect(states(mk(split))).toEqual({ 'access-1': 'up', 'core-1': 'mismatch', 'core-2': 'mismatch' })
  })

  it('marks an edge down when the NIC link is down or the switch port is admin-down', () => {
    const dead = host('dead', 'access-1', 1)
    dead.inventory.nics[0].link_up = false
    const off = host('off', 'access-1', 2)
    const access = sw('access-1', ['p2'])
    access.ports[0].cfg = { ...cfg, admin_up: false }
    const { edges } = layoutTopology([access, ...cores], [dead, off], { collapseOver: 24 })
    expect(edges.find((e) => e.a === 'dead' && e.b === 'access-1')).toMatchObject({ state: 'down', utilisation: 0 })
    expect(edges.find((e) => e.a === 'off' && e.b === 'access-1')!.state).toBe('down')
  })

  it('utilisation is the busier direction over the link capacity, from the host-side port', () => {
    const loads: PortLoad[] = [
      { port: { HostPort: ['h1', 'eth0'] }, rx_bps: 250e6, tx_bps: 1e9 },
      { port: { SwitchPort: ['access-1', 'sfp1'] }, rx_bps: 5e9, tx_bps: 1e9 },
      { port: { HostPort: ['h2', 'eth0'] }, rx_bps: 9e9, tx_bps: 0 },
    ]
    const { edges } = layoutTopology([sw('access-1', ['sfp1', 'sfp2']), ...cores], [host('h1', 'access-1', 1), host('h2', 'access-1', 2)], {
      collapseOver: 24,
      ports: loads,
    })
    const util = (a: string, b: string) => edges.find((e) => e.a === a && e.b === b)!.utilisation
    expect(util('h1', 'access-1')).toBeCloseTo(0.4)
    expect(util('h2', 'access-1')).toBe(1) // clamped
    expect(util('access-1', 'core-1')).toBeCloseTo(0.5) // sfp1 -> first core, 10G
    expect(util('access-1', 'core-2')).toBe(0)
  })

  it('a collapsed group takes the busiest and worst of its hosts', () => {
    const issue: Issue = { NativeVlanMismatch: { network: 'mgmt', host_port: 'eth0', switch: 'access-1', switch_port: 'p2', vlan: 1, native: null } }
    const hosts = [host('h1', 'access-1', 1), host('h2', 'access-1', 2, [], [issue])]
    const ports: PortLoad[] = [{ port: { HostPort: ['h1', 'eth0'] }, rx_bps: 1.25e9, tx_bps: 0 }]
    const { edges, nodes } = layoutTopology([sw('access-1'), ...cores], hosts, { collapseOver: 1, ports })
    expect(edges.find((e) => e.a === 'group:access-1')).toMatchObject({ utilisation: 0.5, state: 'mismatch' })
    expect(nodes.find((n) => n.id === 'group:access-1')).toMatchObject({ hosts: 2, mismatched: 1, down: 0 })
  })

  it('is deterministic whatever the input order', () => {
    const switches = [sw('access-2'), ...cores, sw('access-1')]
    const hosts = [host('b', 'access-2', 1), host('a', 'access-1', 1), host('c', 'access-1', 2)]
    const one = layoutTopology(switches, hosts, { collapseOver: 24 })
    const two = layoutTopology([...switches].reverse(), [...hosts].reverse(), { collapseOver: 24 })
    expect(two).toEqual(one)
  })

  it('leaves out hosts that have not reported their cabling', () => {
    const lost = { ...host('lost', 'access-1', 1), lldp: [] } as HostObj
    const { nodes } = layoutTopology([sw('access-1'), ...cores], [lost], { collapseOver: 24 })
    expect(nodes.some((n) => n.id === 'lost')).toBe(false)
  })
})

describe('utilisationBand', () => {
  it('splits at 50% and 80%', () => {
    expect([0, 0.5, 0.51, 0.8, 0.81, 1].map(utilisationBand)).toEqual(['low', 'low', 'mid', 'mid', 'high', 'high'])
  })
})

describe('suggestedFixes', () => {
  const alert = (host: string, resolved_at: number | null = null, kind = 'NetworkMismatch') =>
    ({ id: `a-${host}`, kind, entity: { kind: 'Host', id: host }, resolved_at }) as Alert
  const withFixes = (id: string, sw: string, fixes: string[]) => ({ ...host(id, sw, 1), net: { issues: [], fixes } }) as unknown as HostObj

  it('lists this switch\'s fixes for cabled hosts with an open mismatch alert', () => {
    const hosts = [
      withFixes('a', 'access-1', ['access-1 p7: add VLAN 20 to trunk allowed', 'core-1 p3: set LACP group a', 'a bond0: cable it']),
      withFixes('b', 'access-1', ['access-1 p9: set untagged VLAN to 1 (is none)']),
      withFixes('c', 'access-2', ['access-1 p1: not mine']),
    ]
    const fixes = suggestedFixes('access-1', hosts, [alert('a'), alert('b', 5), alert('c')])
    expect(fixes).toEqual([{ host: 'a', text: 'access-1 p7: add VLAN 20 to trunk allowed', port: 'p7' }])
    expect(suggestedFixes('access-1', hosts, [alert('a', null, 'HostDown')])).toEqual([])
  })
})
