import { describe, expect, it } from 'vitest'
import type { HostObj } from '../gen/HostObj'
import { replaceChoices, replaceSteps } from './ReplaceDialog'

describe('replacing a down control-plane voter', () => {
  const ms01 = { id: 'Ms01', arch: 'X86_64', ram_gb: 96 }
  const mini = { id: 'MiniPc', arch: 'X86_64', ram_gb: 32 }
  const pi = { id: 'Pi5', arch: 'Arm64', ram_gb: 8 }
  const host = (id: string, profile: object, lifecycle = 'Ready') => ({ id, lifecycle, profile: { cpu_threads: 4, disks: [], capabilities: [], ...profile } }) as unknown as HostObj
  const hosts = Object.fromEntries(
    [host('v1', ms01), host('v2', ms01, 'NotReady'), host('v3', ms01), host('a-mini', mini), host('z-ms01', ms01), host('cordoned', ms01, 'Cordoned'), host('pi', pi)].map((h) => [h.id, h]),
  )
  const cp = { voters: ['v1', 'v2', 'v3'], mode: 'Suggest' as const, handoff: null, version: 1 }

  it('offers a down voter every host that may join, MS-01s first', () => {
    expect(replaceChoices(hosts.v2, cp, hosts)).toEqual([
      { dead: 'v2', with: 'z-ms01' },
      { dead: 'v2', with: 'a-mini' },
    ])
  })

  it('offers a host that may join every down voter, and live voters and other hosts nothing', () => {
    expect(replaceChoices(hosts['a-mini'], cp, hosts)).toEqual([{ dead: 'v2', with: 'a-mini' }])
    expect(replaceChoices(hosts.v1, cp, hosts)).toEqual([])
    expect(replaceChoices(hosts.cordoned, cp, hosts)).toEqual([])
    expect(replaceChoices(hosts.pi, cp, hosts)).toEqual([])
    const allUp: Record<string, HostObj> = { ...hosts, v2: host('v2', ms01) }
    expect(replaceChoices(allUp['a-mini'], cp, allUp)).toEqual([])
  })

  it('follows the replacement in the order the control plane runs it', () => {
    const r = { dead: 'v2', with: 'z-ms01' }
    const states = (voters: string[], spare: HostObj) => replaceSteps(r, { ...cp, voters }, { ...hosts, 'z-ms01': spare }).map((s) => [s.state, s.detail])
    const k3s = (phase: 'Running' | 'Installing', detail: string | null = null) => ({ ...hosts['z-ms01'], kube: { role: 'Server', phase, detail } }) as unknown as HostObj
    expect(states(['v1', 'v2', 'v3'], hosts['z-ms01'])).toEqual([['active', null], ['pending', null], ['pending', null]])
    expect(states(['v1', 'v2', 'v3', 'z-ms01'], hosts['z-ms01'])).toEqual([['done', null], ['active', null], ['pending', null]])
    expect(states(['v1', 'v3', 'z-ms01'], k3s('Installing', 'installing K3s as Server'))).toEqual([['done', null], ['done', null], ['active', 'installing K3s as Server']])
    expect(states(['v1', 'v3', 'z-ms01'], k3s('Running')).every(([s]) => s === 'done')).toBe(true)
  })
})
