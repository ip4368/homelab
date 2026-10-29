import { describe, expect, it } from 'vitest'
import type { Event as ClusterEvent } from '../gen/Event'
import type { HostObj } from '../gen/HostObj'
import { handoffChoices, handoffEndReason, handoffSteps } from './HandoffDialog'

describe('control-plane hand-off', () => {
  const ms01 = { id: 'Ms01', arch: 'X86_64', ram_gb: 96 }
  const pi = { id: 'Pi5', arch: 'Arm64', ram_gb: 8 }
  const host = (id: string, profile: object, lifecycle = 'Ready') => ({ id, lifecycle, profile: { cpu_threads: 4, disks: [], capabilities: [], ...profile } }) as unknown as HostObj
  const hosts = Object.fromEntries(
    [host('v1', ms01), host('v2', ms01), host('spare', ms01), host('cordoned', ms01, 'Cordoned'), host('pi', pi)].map((h) => [h.id, h]),
  )
  const cp = { voters: ['v2', 'v1'], mode: 'Suggest' as const, handoff: null, version: 1 }

  it('offers a voter every Ready, eligible host that is no voter to hand its place to', () => {
    expect(handoffChoices(hosts.v1, cp, hosts)).toEqual([{ from: 'v1', to: 'spare' }])
  })

  it('offers a host that may join every voter to take the place of, and others nothing', () => {
    expect(handoffChoices(hosts.spare, cp, hosts)).toEqual([
      { from: 'v1', to: 'spare' },
      { from: 'v2', to: 'spare' },
    ])
    expect(handoffChoices(hosts.cordoned, cp, hosts)).toEqual([])
    expect(handoffChoices(hosts.pi, cp, hosts)).toEqual([])
  })

  it('follows the hand-off step by step from what the cluster state shows', () => {
    const k3s = (h: HostObj, role: 'Server' | 'Agent', phase: 'Running' | 'Installing', detail: string | null = null) =>
      ({ ...h, kube: { role, phase, detail } }) as unknown as HostObj
    const states = (v: string[], from: HostObj, to: HostObj) =>
      handoffSteps({ from: 'v1', to: 'spare' }, { ...cp, voters: v }, { v1: from, spare: to }).map((s) => [s.state, s.detail])
    const [server, agent] = [k3s(hosts.v1, 'Server', 'Running'), k3s(hosts.spare, 'Agent', 'Running')]
    expect(states(['v1', 'v2'], server, agent)).toEqual([['active', null], ['pending', null], ['pending', null], ['pending', null]])
    const installing = k3s(hosts.spare, 'Server', 'Installing', 'installing K3s as Server')
    expect(states(['v1', 'v2', 'spare'], server, installing)).toEqual([['done', null], ['active', 'installing K3s as Server'], ['pending', null], ['pending', null]])
    const reinstalling = k3s(hosts.v1, 'Agent', 'Installing', 'installed; starting')
    // Once v1 has left, the server step counts as done even if spare is restarting.
    expect(states(['v2', 'spare'], reinstalling, installing)).toEqual([['done', null], ['done', null], ['done', null], ['active', 'installed; starting']])
    expect(states(['v2', 'spare'], k3s(hosts.v1, 'Agent', 'Running'), k3s(hosts.spare, 'Server', 'Running')).every(([s]) => s === 'done')).toBe(true)
  })

  it('says why a hand-off ended without its old voter leaving, from the newest event about it', () => {
    const event = (seq: number, kind: string, message: string) => ({ seq, at: seq, entity: { kind, id: '' }, message, version: seq }) as unknown as ClusterEvent
    const events = [
      event(1, 'Cp', "gave up handing v1's control-plane place to spare: spare cannot join (it must be Ready, eligible and not a member)"),
      event(2, 'Cp', "handing v1's control-plane place to spare"),
      event(3, 'Cp', "cancelling the hand-off of v1's control-plane place to spare"),
      event(4, 'Cp', "stopped handing v1's control-plane place to spare: cancelled by the user"),
      event(5, 'Host', "stopped handing v1's control-plane place to spare: not the control plane's"),
      event(6, 'Cp', "stopped handing v2's control-plane place to spare: cancelled by the user"),
    ]
    expect(handoffEndReason({ from: 'v1', to: 'spare' }, events)).toBe("stopped handing v1's control-plane place to spare: cancelled by the user")
    expect(handoffEndReason({ from: 'v1', to: 'spare' }, events.slice(0, 3))).toMatch(/^gave up handing v1's/)
    expect(handoffEndReason({ from: 'v1', to: 'other' }, events)).toBeUndefined()
  })
})
