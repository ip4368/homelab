import { describe, expect, it } from 'vitest'
import type { HostObj } from '../gen/HostObj'
import { replaceChoices, replaceSteps } from './ReplaceDialog'

describe('control-plane replacement', () => {
  const ms01 = { id: 'Ms01', arch: 'X86_64', ram_gb: 96 }
  const mini = { id: 'MiniPc', arch: 'X86_64', ram_gb: 16 }
  const pi = { id: 'Pi5', arch: 'Arm64', ram_gb: 8 }
  const host = (id: string, profile: object, location: string, lifecycle = 'Ready') =>
    ({ id, lifecycle, location, profile: { cpu_threads: 4, disks: [], capabilities: [], ...profile } }) as unknown as HostObj
  const hosts = Object.fromEntries(
    [
      host('v1', ms01, 'rack-0', 'NotReady'),
      host('v2', ms01, 'rack-0'),
      host('spare-ms01', ms01, 'rack-0'),
      host('spare-mini', mini, 'rack-1'),
      host('cordoned', ms01, 'rack-2', 'Cordoned'),
      host('pi', pi, 'rack-2'),
    ].map((h) => [h.id, h]),
  )
  const cp = { voters: ['v1', 'v2'], mode: 'Suggest' as const, handoff: null, version: 1 }

  it('offers Ready, eligible non-voters best first: MS-01s, then an unused rack, then id', () => {
    expect(replaceChoices(cp, hosts)).toEqual(['spare-ms01', 'spare-mini'])
  })

  it('prefers a rack no voter uses once the MS-01s are gone', () => {
    const racked: Record<string, HostObj> = {
      v1: hosts.v1,
      v2: hosts.v2,
      'spare-mini': hosts['spare-mini'],
      'spare-rack0': host('spare-rack0', mini, 'rack-0'),
    }
    expect(replaceChoices(cp, racked)).toEqual(['spare-mini', 'spare-rack0'])
  })

  it('follows the replacement from what the cluster state shows', () => {
    const states = (v: string[]) => replaceSteps({ dead: 'v1', with: 'spare-ms01' }, { ...cp, voters: v }).map((s) => s.state)
    expect(states(['v1', 'v2'])).toEqual(['active', 'pending'])
    expect(states(['v1', 'v2', 'spare-ms01'])).toEqual(['done', 'active'])
    expect(states(['v2', 'spare-ms01'])).toEqual(['done', 'done'])
  })
})
