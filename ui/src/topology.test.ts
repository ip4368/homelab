import { describe, expect, it } from 'vitest'
import type { Alert } from './gen/Alert'
import type { HostObj } from './gen/HostObj'
import type { Issue } from './gen/Issue'
import type { PortTruth } from './gen/PortTruth'
import type { SwitchObj } from './gen/SwitchObj'
import { layoutTopology, suggestedFixes, utilisationBand } from './topology'

const cfg = { mode: { Access: 1 }, mtu: 1500, lacp_group: null, admin_up: true } as const

const counters = { rx_bytes: 0, tx_bytes: 0, rx_frames: 0, tx_frames: 0, drop_vlan: 0, drop_mtu: 0, drop_rate: 0, drop_partition: 0, drop_lacp: 0 }
/** A switch port's measured load, with the host NIC cabled to it if any. */
const load = (sw: string, port: string, rx_bps: number, tx_bps: number, host: string | null = null, nic: string | null = null): PortTruth => ({
  switch: sw,
  port,
  host,
  nic,
  link_up: true,
  rx_bps,
  tx_bps,
  counters,
  vlan_rx_bytes: {},
})

function sw(id: string, ports: string[] = [], mlag_peer: string | null = null): SwitchObj {
  return {
    id,
    model: 'm',
    mlag_peer,
    last_seen: 0,
    version: 1,
    ports: ports.map((port) => ({ switch: id, port, cfg, mlag_peer, link_up: true, speed_mbps: 2500, neighbor: null })),
  }
}

/** Cables `a`'s port `pa` to `b`'s port `pb`, as both switches would report it over LLDP. */
function cable(a: SwitchObj, pa: string, b: SwitchObj, pb: string, speed_mbps = 10_000) {
  for (const [x, px, y, py] of [[a, pa, b, pb], [b, pb, a, pa]] as const) {
    const port = x.ports.find((p) => p.port === px) ?? (x.ports.push({ switch: x.id, port: px, cfg, mlag_peer: x.mlag_peer, link_up: true, speed_mbps, neighbor: null }), x.ports.at(-1)!)
    port.speed_mbps = speed_mbps
    port.neighbor = { switch: y.id, port: py }
  }
}

/** core-1 and core-2 joined by a 40G link, with access switches dual-homed to both. */
function fabric(...accessIds: string[]) {
  const [c1, c2] = [sw('core-1', [], 'core-2'), sw('core-2', [], 'core-1')]
  cable(c1, 'qsfp1', c2, 'qsfp1', 40_000)
  const access = accessIds.map((id, i) => {
    const a = sw(id)
    cable(a, 'sfp1', c1, `p${60 + i}`)
    cable(a, 'sfp2', c2, `p${60 + i}`)
    return a
  })
  return { cores: [c1, c2], access, all: [...access, c1, c2] }
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
    const { nodes, edges } = layoutTopology(fabric('access-1').all, [ms01, host('pi5-01', 'access-1', 2)], { collapseOver: 24 })
    const x = (id: string) => nodes.find((n) => n.id === id)!.x
    expect(Math.abs(x('ms01-01') - x('access-1'))).toBeLessThan(200)
    const legs = edges.filter((e) => e.a === 'ms01-01').map((e) => e.b).sort()
    expect(legs).toEqual(['access-1', 'core-1', 'core-2'])
  })

  it('draws switch-to-switch edges from the reported neighbours, each cable once', () => {
    const f = fabric('access-1', 'access-2')
    const hosts = [host('h1', 'access-1', 1), host('h2', 'access-2', 1)]
    const { edges } = layoutTopology(f.all, hosts, { collapseOver: 24 })
    const links = edges.filter((e) => e.a.match(/^(access|core)-/) && e.b.startsWith('core-'))
    expect(links.map((e) => [e.a, e.b, e.switchPort, e.farPort])).toEqual([
      ['access-1', 'core-1', 'sfp1', 'p60'],
      ['access-1', 'core-2', 'sfp2', 'p60'],
      ['access-2', 'core-1', 'sfp1', 'p61'],
      ['access-2', 'core-2', 'sfp2', 'p61'],
      ['core-1', 'core-2', 'qsfp1', 'qsfp1'],
    ])
    // Cables to a switch the control plane does not know are not drawn.
    const lonely = layoutTopology(f.all.filter((x) => x.id !== 'core-2'), hosts, { collapseOver: 24 })
    expect(lonely.edges.some((e) => e.b === 'core-2' || e.a === 'core-2')).toBe(false)
  })

  it('takes a switch with an MLAG peer, or no host mgmt NIC, for a core', () => {
    const lone = sw('spine-1')
    const a = sw('access-1')
    cable(a, 'sfp1', lone, 'p1')
    const { nodes } = layoutTopology([a, lone], [host('h1', 'access-1', 1)], { collapseOver: 24 })
    expect(nodes.filter((n) => n.kind === 'core').map((n) => n.id)).toEqual(['spine-1'])
  })

  it('capacity of a switch link is the slower end; load comes from either end', () => {
    const f = fabric('access-1')
    cable(f.access[0], 'sfp1', f.cores[0], 'p60', 2_500) // a degraded cable
    const ports = [load('core-1', 'p60', 0, 1.25e9), load('core-1', 'qsfp1', 10e9, 2e9)]
    const { edges } = layoutTopology(f.all, [host('h1', 'access-1', 1)], { collapseOver: 24, ports })
    const util = (a: string, b: string) => edges.find((e) => e.a === a && e.b === b)!.utilisation
    expect(util('access-1', 'core-1')).toBeCloseTo(0.5)
    expect(util('core-1', 'core-2')).toBeCloseTo(0.25) // 10G of 40G
    expect(util('access-1', 'core-2')).toBe(0)
  })

  it('a switch link is down when either end has no carrier or is admin-down, or a switch is unreachable', () => {
    const state = (f: ReturnType<typeof fabric>, unreachable?: Set<string>) => {
      const { edges } = layoutTopology(f.all, [host('h1', 'access-1', 1)], { collapseOver: 24, unreachable })
      return Object.fromEntries(edges.filter((e) => e.farPort).map((e) => [`${e.a}-${e.b}`, e.state]))
    }
    const healthy = { 'access-1-core-1': 'up', 'access-1-core-2': 'up', 'core-1-core-2': 'up' }
    expect(state(fabric('access-1'))).toEqual(healthy)

    const carrier = fabric('access-1') // the live end of a dead core sees no carrier
    carrier.access[0].ports.find((p) => p.port === 'sfp2')!.link_up = false
    expect(state(carrier)).toEqual({ ...healthy, 'access-1-core-2': 'down' })

    const far = fabric('access-1') // only the far end reports it
    far.cores[1].ports.find((p) => p.port === 'p60')!.link_up = false
    expect(state(far)).toEqual({ ...healthy, 'access-1-core-2': 'down' })

    const admin = fabric('access-1')
    admin.cores[0].ports.find((p) => p.port === 'qsfp1')!.cfg = { ...cfg, admin_up: false }
    expect(state(admin)).toEqual({ ...healthy, 'core-1-core-2': 'down' })

    // A silent switch never reports its own links down: the alert marks them.
    expect(state(fabric('access-1'), new Set(['core-2']))).toEqual({ ...healthy, 'access-1-core-2': 'down', 'core-1-core-2': 'down' })
    const { nodes } = layoutTopology(fabric('access-1').all, [host('h1', 'access-1', 1)], { unreachable: new Set(['core-2']) })
    expect(nodes.find((n) => n.id === 'core-2')!.unreachable).toBe(true)
    expect(nodes.find((n) => n.id === 'core-1')!.unreachable).toBe(false)
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

  it('flags a switch-side MTU mismatch on that port, but not a host link MTU', () => {
    const switchSide: Issue = { MtuTooSmall: { network: 'storage', where_: 'access-1 p1', have: 1500, need: 9000 } }
    const otherPort: Issue = { MtuTooSmall: { network: 'storage', where_: 'access-1 p2', have: 1500, need: 9000 } }
    const hostSide: Issue = { MtuTooSmall: { network: 'storage', where_: 'link eth0', have: 1500, need: 9000 } }
    const hosts = [host('a', 'access-1', 1, [], [switchSide]), host('b', 'access-1', 3, [], [otherPort, hostSide])]
    const { edges } = layoutTopology([sw('access-1'), ...cores], hosts, { collapseOver: 24 })
    const state = (h: string) => edges.find((e) => e.a === h && e.b === 'access-1')!.state
    expect([state('a'), state('b')]).toEqual(['mismatch', 'up'])
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

  it('host edge utilisation is the busier direction over the NIC speed, from the port the NIC is cabled to', () => {
    const loads = [load('access-1', 'p1', 1e9, 250e6, 'h1', 'eth0'), load('access-1', 'p2', 0, 9e9, 'h2', 'eth0')]
    const { edges } = layoutTopology(fabric('access-1').all, [host('h1', 'access-1', 1), host('h2', 'access-1', 2)], {
      collapseOver: 24,
      ports: loads,
    })
    const util = (a: string, b: string) => edges.find((e) => e.a === a && e.b === b)!.utilisation
    expect(util('h1', 'access-1')).toBeCloseTo(0.4)
    expect(util('h2', 'access-1')).toBe(1) // clamped
  })

  it('a collapsed group takes the busiest and worst of its hosts', () => {
    const issue: Issue = { NativeVlanMismatch: { network: 'mgmt', host_port: 'eth0', switch: 'access-1', switch_port: 'p2', vlan: 1, native: null } }
    const hosts = [host('h1', 'access-1', 1), host('h2', 'access-1', 2, [], [issue])]
    const ports = [load('access-1', 'p1', 1.25e9, 0, 'h1', 'eth0')]
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
    const stray = host('stray', 'nowhere', 1) // cabled to a switch nobody has heard of
    const { nodes, unplaced } = layoutTopology([sw('access-1'), ...cores], [lost, stray, host('h', 'access-1', 1)], { collapseOver: 24 })
    expect(nodes.some((n) => n.id === 'lost' || n.id === 'stray')).toBe(false)
    expect(unplaced).toBe(2)
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
