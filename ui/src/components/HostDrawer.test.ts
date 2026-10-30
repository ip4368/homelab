import { describe, expect, it } from 'vitest'
import type { HostNetworkConfig } from '../gen/HostNetworkConfig'
import { apiHealthLine, kubeSummary, linkPorts, nicState, repairLabel } from './HostDrawer'

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
    etcd_unread: null,
    etcd_unstarted: [],
    storage_off: [],
    cordoned: [],
    cordon_noted: [],
    drained_since: {},
    drain_blocked: {},
    ...over,
  })
  it('says what the host runs and how far it got', () => {
    expect(kubeSummary(null).tone).toBe('muted')
    expect(kubeSummary(k({}))).toEqual({ text: 'K3s server (API + etcd) v1.36.5+k3s1, Node Ready, Longhorn on nvme0', tone: 'ok' })
    expect(kubeSummary(k({ node_ready: false })).text).toContain('Node not Ready')
    expect(kubeSummary(k({ role: 'Agent', phase: 'Installing', detail: 'starting k3s-agent' })).text).toBe('K3s agent: starting k3s-agent')
    // A reinstall reports the role still installed, and the detail names the one coming.
    expect(kubeSummary(k({ phase: 'Installing', detail: 'installing K3s as Agent' })).text).toBe('switching from K3s server (API + etcd) to K3s agent')
    expect(kubeSummary(k({ role: 'Agent', phase: 'Installing', detail: 'installing K3s as Server' })).text).toBe('switching from K3s agent to K3s server (API + etcd)')
    expect(kubeSummary(k({ role: null, phase: 'Installing', detail: 'installing K3s as Server' })).text).toBe('installing K3s server (API + etcd)')
    expect(kubeSummary(k({ phase: { Failed: 'no route' } })).tone).toBe('bad')
    expect(kubeSummary(k({ etcd_unread: 'curl: (7) Failed to connect' }))).toEqual({
      text: "K3s server (API + etcd) v1.36.5+k3s1, Node Ready, Longhorn on nvme0; cannot read etcd's members (curl: (7) Failed to connect)",
      tone: 'warn',
    })
  })

  it('says how the server API stands, and nothing for a host with no server', () => {
    expect(apiHealthLine('Server', { api_ready: true })).toEqual({ text: 'ready', tone: 'ok' })
    expect(apiHealthLine('Server', { api_ready: false })).toEqual({ text: 'not ready', tone: 'bad' })
    expect(apiHealthLine('Server', undefined)).toEqual({ text: 'health unknown (no report yet)', tone: 'muted' })
    expect(apiHealthLine('Agent', { api_ready: true })).toBeNull()
    expect(apiHealthLine(null, undefined)).toBeNull()
  })

  it('names repair attempts with their age', () => {
    expect(repairLabel({ at: 1000, action: 'RestartK3s' }, 13_000)).toBe('restarted K3s 12s ago')
    expect(repairLabel({ at: 0, action: 'RestartK3s' }, 12 * 60_000)).toBe('restarted K3s 12m ago')
  })
})
