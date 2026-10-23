import { describe, expect, it } from 'vitest'
import type { HostObj } from '../gen/HostObj'
import { formatClock } from '../clock'
import { quorumOf } from './TopBar'

const host = (lifecycle: HostObj['lifecycle']) => ({ lifecycle }) as HostObj

describe('TopBar helpers', () => {
  it('formats the sim clock as hh:mm:ss, past 24 hours too', () => {
    expect(formatClock(0)).toBe('00:00:00')
    expect(formatClock(3_725_999)).toBe('01:02:05')
    expect(formatClock(100 * 3_600_000)).toBe('100:00:00')
  })

  it('counts live voters: present in the view and not NotReady', () => {
    const cp = { voters: ['a', 'b', 'c'], mode: 'Suggest' as const, version: 1 }
    const hosts = { a: host('Ready'), b: host('NotReady'), c: host('Ready') }
    expect(quorumOf(cp, hosts)).toEqual({ live: 2, voters: 3, ok: true })
    expect(quorumOf(cp, { a: host('Ready') })).toEqual({ live: 1, voters: 3, ok: false })
    expect(quorumOf({ ...cp, voters: [] }, {})).toEqual({ live: 0, voters: 0, ok: false })
  })
})
