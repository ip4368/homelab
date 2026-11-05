import { describe, expect, it, vi } from 'vitest'
import type { HostNetworkConfig } from '../gen/HostNetworkConfig'
import type { HostObj } from '../gen/HostObj'
import type { ServerMsg } from '../gen/ServerMsg'
import type { ViewSnapshot } from '../gen/ViewSnapshot'
import { createStore, emptyKube } from '../store'
import { kubeSummary, leadership, linkPorts, MOVE_VIP_WAIT_MS, nicState, untilLeads } from './HostDetail'

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
    kvm: true,
    kubevirt: false,
    ...over,
  })
  it('says what the host runs and how far it got', () => {
    expect(kubeSummary(null).tone).toBe('muted')
    expect(kubeSummary(k({}))).toEqual({ text: 'K3s server (API + etcd) v1.36.5+k3s1, node Ready, Longhorn on nvme0', tone: 'ok' })
    expect(kubeSummary(k({ role: 'Agent', phase: 'Installing', detail: 'starting k3s-agent' })).text).toBe('K3s agent: starting k3s-agent')
    // A reinstall reports the role still installed, and the detail names the one coming.
    expect(kubeSummary(k({ phase: 'Installing', detail: 'installing K3s as Agent' })).text).toBe('switching from K3s server (API + etcd) to K3s agent')
    expect(kubeSummary(k({ role: 'Agent', phase: 'Installing', detail: 'installing K3s as Server' })).text).toBe('switching from K3s agent to K3s server (API + etcd)')
    expect(kubeSummary(k({ role: null, phase: 'Installing', detail: 'installing K3s as Server' })).text).toBe('installing K3s server (API + etcd)')
    expect(kubeSummary(k({ phase: { Failed: 'no route' } })).tone).toBe('bad')
    expect(kubeSummary(k({ etcd_unread: 'curl: (7) Failed to connect' }))).toEqual({
      text: "K3s server (API + etcd) v1.36.5+k3s1, node Ready, Longhorn on nvme0; cannot read etcd's members (curl: (7) Failed to connect)",
      tone: 'warn',
    })
  })
})

describe('host drawer leadership', () => {
  const host = (id: string, lifecycle: HostObj['lifecycle'] = 'Ready') => ({ id, lifecycle }) as HostObj
  const voters = ['v1', 'v2', 'v3']
  it('offers to move the VIP to a voter that is up, and says which voter leads', () => {
    expect(leadership(host('v1'), voters, 'v1')).toBe('leads')
    expect(leadership(host('v2'), voters, 'v1')).toBe('offer')
    expect(leadership(host('v2', 'Cordoned'), voters, 'v1')).toBe('offer')
    expect(leadership(host('v3', 'NotReady'), voters, 'v1')).toBe('down')
  })
  it('offers nothing on a non-voter, or with no leader to ask', () => {
    expect(leadership(host('m1'), voters, 'v1')).toBeNull()
    expect(leadership(host('v2'), voters, null)).toBeNull()
  })
})

describe('moving the VIP', () => {
  const led = (leader: string): ServerMsg => {
    const view: ViewSnapshot = {
      version: 1,
      leader,
      term: 1,
      last_sync_at: 0,
      hosts: [],
      tokens: [],
      rules: [],
      switches: [],
      networks: [],
      templates: [],
      rollouts: [],
      alerts: [],
      events: [],
      cp: { voters: ['v1', 'v2', 'v3'], mode: 'Suggest', handoff: null, version: 1 },
      metrics: {},
    }
    return { type: 'snapshot', view, truth: { vms: [], switches: [], ports: [], partitions: [], vip_holders: [], at: 0 }, kube: emptyKube, clock: { now: 0 } }
  }

  it('waits for the voter to lead', async () => {
    vi.useFakeTimers()
    const s = createStore()
    s.applyMessage(led('v1'))
    let done = false
    const wait = untilLeads(s, 'v2').then(() => (done = true))
    await vi.advanceTimersByTimeAsync(1000)
    expect(done).toBe(false)
    s.applyMessage(led('v2'))
    await wait
    expect(done).toBe(true)
    vi.useRealTimers()
  })

  it('says so when the voter does not take over', async () => {
    vi.useFakeTimers()
    const s = createStore()
    s.applyMessage(led('v1'))
    const wait = expect(untilLeads(s, 'v2')).rejects.toBe('v2 did not take over: v1 still leads. Try again, or check that v2 is healthy.')
    await vi.advanceTimersByTimeAsync(MOVE_VIP_WAIT_MS)
    await wait
    vi.useRealTimers()
  })
})
