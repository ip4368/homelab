import { describe, expect, it } from 'vitest'
import type { HostNetworkConfig } from './gen/HostNetworkConfig'
import {
  addAttachment,
  addBond,
  addSingle,
  configDiff,
  describeIssue,
  formFromNetwork,
  networkFromForm,
  formProblems,
  moveAttachment,
  parseInUse,
  removeLink,
  setIp,
  setTagged,
} from './netedit'

const cfg = (): HostNetworkConfig => ({
  links: [
    { name: 'eth0', kind: { Single: 'eth0' }, mtu: 1500 },
    { name: 'sfp0', kind: { Single: 'sfp0' }, mtu: 9000 },
    { name: 'sfp1', kind: { Single: 'sfp1' }, mtu: 9000 },
  ],
  attachments: [
    { network: 'mgmt', link: 'eth0', tagged: false, ip: '10.0.0.5' },
    { network: 'storage', link: 'sfp0', tagged: true, ip: '10.20.0.5' },
    { network: 'migration', link: 'sfp1', tagged: true, ip: '10.30.0.5' },
  ],
})

describe('netedit', () => {
  it('addBond removes members from single links', () => {
    const before = cfg()
    const out = addBond(before, 'bond0', ['sfp0', 'sfp1'], 'Lacp')
    expect(out.links.map((l) => l.name)).toEqual(['eth0', 'bond0'])
    expect(out.links[1]).toEqual({ name: 'bond0', kind: { Bond: { members: ['sfp0', 'sfp1'], mode: 'Lacp' } }, mtu: 9000 })
    // The members' networks ride the bond now, addresses intact.
    expect(out.attachments.filter((a) => a.link === 'bond0').map((a) => [a.network, a.ip])).toEqual([
      ['storage', '10.20.0.5'],
      ['migration', '10.30.0.5'],
    ])
    expect(before).toEqual(cfg())
  })

  it('addBond keeps one attachment per network when both members carried it', () => {
    const c = addAttachment(cfg(), 'vm-lan', 'sfp0', true)
    c.attachments.push({ network: 'vm-lan', link: 'sfp1', tagged: true, ip: null })
    const out = addBond(c, 'bond0', ['sfp0', 'sfp1'], 'ActiveBackup')
    expect(out.attachments.filter((a) => a.network === 'vm-lan')).toHaveLength(1)
  })

  it('removeLink drops its attachments', () => {
    const out = removeLink(cfg(), 'sfp0')
    expect(out.links.map((l) => l.name)).toEqual(['eth0', 'sfp1'])
    expect(out.attachments.map((a) => a.network)).toEqual(['mgmt', 'migration'])
  })

  it('moveAttachment keeps ip', () => {
    const out = moveAttachment(cfg(), 'storage', 'sfp1')
    expect(out.attachments.find((a) => a.network === 'storage')).toEqual({ network: 'storage', link: 'sfp1', tagged: true, ip: '10.20.0.5' })
  })

  it('setTagged and setIp touch only their network', () => {
    const out = setIp(setTagged(cfg(), 'mgmt', true), 'storage', ' ')
    expect(out.attachments.map((a) => [a.network, a.tagged, a.ip])).toEqual([
      ['mgmt', true, '10.0.0.5'],
      ['storage', true, null],
      ['migration', true, '10.30.0.5'],
    ])
  })

  it('addSingle skips a port a link already uses', () => {
    expect(addSingle(cfg(), 'sfp0')).toEqual(cfg())
    expect(addSingle(cfg(), 'sfp2').links.at(-1)).toEqual({ name: 'sfp2', kind: { Single: 'sfp2' }, mtu: 1500 })
  })

  it('formProblems finds what the server would refuse for lack of structure', () => {
    expect(formProblems(cfg())).toEqual([])
    const bad = addBond(cfg(), 'bond0', ['sfp0'], 'Lacp')
    expect(formProblems(bad)).toEqual(['Bond bond0 needs at least two ports'])
    const orphan = { ...cfg(), attachments: [{ network: 'x', link: 'nope', tagged: false, ip: null }] }
    expect(formProblems(orphan)).toEqual(['Network x is on a link that does not exist'])
  })

  it('configDiff names what changed and ignores addresses', () => {
    const template = { ...cfg(), attachments: cfg().attachments.map((a) => ({ ...a, ip: null })) }
    expect(configDiff(template, cfg())).toEqual([])
    const edited = setTagged(removeLink(cfg(), 'sfp1'), 'mgmt', true)
    expect(configDiff(template, edited)).toEqual(['link sfp1 removed', 'mgmt: untagged -> tagged', 'migration detached'])
  })

  it('parseInUse splits the users of a refused delete', () => {
    expect(parseInUse('in use by rule pi5, rule rk3588')).toEqual(['rule pi5', 'rule rk3588'])
    expect(parseInUse('in use by template ms01-standard')).toEqual(['template ms01-standard'])
    expect(parseInUse('not found')).toBeNull()
  })

  it('parseInUse finds the refusal inside a longer message', () => {
    expect(parseInUse('rejected: in use by host a, host b')).toEqual(['host a', 'host b'])
  })

  it('describeIssue names the networks and ports involved', () => {
    expect(describeIssue({ VlanNotAllowed: { network: 'storage', host_port: 'sfp0', switch: 'core-1', switch_port: 'p3', vlan: 20 } })).toBe(
      'storage: core-1 p3 does not allow VLAN 20 (host port sfp0)',
    )
    expect(describeIssue({ PortNotCabled: { port: 'sfp1', networks: ['storage', 'migration'] } })).toBe('port sfp1 is not cabled (carries storage, migration)')
    expect(describeIssue({ UnknownNetwork: { id: 'x' } })).toBe('network x does not exist')
  })

  it('networkFromForm round-trips a network and names the bad field', () => {
    const n = { id: 'storage', purpose: 'Storage' as const, vlan: 20, subnet: '10.20.0.0/22', mtu: 9000, reserved: [{ first: '10.20.3.0', last: '10.20.3.255' }], dns: [] }
    expect(networkFromForm(formFromNetwork(n))).toEqual({ network: n })
    const f = formFromNetwork(n)
    expect(networkFromForm({ ...f, vlan: '5000' })).toEqual({ error: 'VLAN: "5000" is not a VLAN id (1-4094)' })
    expect(networkFromForm({ ...f, subnet: '10.20.0.0' })).toMatchObject({ error: expect.stringContaining('Subnet') })
    expect(networkFromForm({ ...f, subnet: '10.300.0.0/22' })).toMatchObject({ error: expect.stringContaining('Subnet') })
    expect(networkFromForm({ ...f, mtu: '100' })).toMatchObject({ error: expect.stringContaining('MTU') })
    expect(networkFromForm({ ...f, id: ' ' })).toEqual({ error: 'Name: required' })
  })
})
