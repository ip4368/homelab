import { describe, expect, it } from 'vitest'
import type { HostNetworkConfig } from '../gen/HostNetworkConfig'
import { linkPorts, nicState } from './HostDrawer'

const cfg: HostNetworkConfig = {
  links: [
    { name: 'bond0', kind: { Bond: { members: ['sfp0', 'sfp1'], mode: 'Lacp' } }, mtu: 9000 },
    { name: 'mgmt', kind: { Single: 'eth0' }, mtu: 1500 },
  ],
  attachments: [],
}
const nic = (name: string, link_up: boolean) => ({ name, speed_mbps: 2500, link_up })

describe('host drawer ports', () => {
  it('shows a port that no link of the applied config uses as unused, not down', () => {
    const used = linkPorts({ applied: cfg, desired: null })
    expect(nicState(nic('eth1', false), used)).toBe('unused')
    expect(nicState(nic('sfp0', false), used)).toBe('down')
    expect(nicState(nic('eth0', true), used)).toBe('up')
    expect(nicState(nic('eth1', true), used)).toBe('up')
  })

  it('falls back to the desired config, and without any config shows link state only', () => {
    expect(nicState(nic('eth1', false), linkPorts({ applied: null, desired: cfg }))).toBe('unused')
    expect(linkPorts({ applied: null, desired: null })).toBeNull()
    expect(nicState(nic('eth1', false), null)).toBe('down')
  })
})
