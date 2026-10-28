import { describe, expect, it } from 'vitest'
import type { HostObj } from '../gen/HostObj'
import { formatClock } from '../clock'
import { quorumOf } from './TopBar'

const host = (lifecycle: HostObj['lifecycle']) => ({ lifecycle }) as HostObj

describe('TopBar helpers', () => {
  it('formats wall-clock milliseconds as local hh:mm:ss', () => {
    expect(formatClock(new Date(2026, 9, 26, 0, 0, 0).getTime())).toBe('00:00:00')
    expect(formatClock(new Date(2026, 9, 26, 1, 2, 5, 999).getTime())).toBe('01:02:05')
    expect(formatClock(new Date(2026, 9, 26, 23, 59, 59).getTime())).toBe('23:59:59')
  })

  it('counts live voters: present in the view and not NotReady', () => {
    const cp = { voters: ['a', 'b', 'c'], mode: 'Suggest' as const, handoff: null, version: 1 }
    const hosts = { a: host('Ready'), b: host('NotReady'), c: host('Ready') }
    expect(quorumOf(cp, hosts)).toEqual({ live: 2, voters: 3, ok: true })
    expect(quorumOf(cp, { a: host('Ready') })).toEqual({ live: 1, voters: 3, ok: false })
    expect(quorumOf({ ...cp, voters: [] }, {})).toEqual({ live: 0, voters: 0, ok: false })
  })
})
