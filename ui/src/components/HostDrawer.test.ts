import { describe, expect, it } from 'vitest'
import type { HostNetworkConfig } from '../gen/HostNetworkConfig'
import { kubeSummary, linkPorts, nicState } from './HostDrawer'

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

describe('host drawer kubernetes', () => {
  const k = (over: Partial<NonNullable<Parameters<typeof kubeSummary>[0]>>) => ({
    role: 'Server' as const,
    phase: 'Running' as const,
    node_ready: true,
    labels: {},
    storage_disks: ['nvme0'],
    version: 'v1.36.5+k3s1',
    detail: null,
    etcd_members: [],
    storage_off: [],
    ...over,
  })
  it('says what the host runs and how far it got', () => {
    expect(kubeSummary(null).tone).toBe('muted')
    expect(kubeSummary(k({}))).toEqual({ text: 'K3s server (API + etcd) v1.36.5+k3s1, node Ready, Longhorn on nvme0', tone: 'ok' })
    expect(kubeSummary(k({ role: 'Agent', phase: 'Installing', detail: 'starting k3s-agent' })).text).toBe('K3s agent: starting k3s-agent')
    expect(kubeSummary(k({ phase: { Failed: 'no route' } })).tone).toBe('bad')
  })
})
