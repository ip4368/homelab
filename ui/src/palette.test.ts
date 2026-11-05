import { describe, expect, it } from 'vitest'
import { matchScore, rank } from './palette'

describe('command palette search', () => {
  it('matches letters in order, preferring a prefix, then a word start, then a substring', () => {
    expect(matchScore('web', 'web')).not.toBeNull()
    expect(matchScore('xyz', 'web')).toBeNull()
    expect(matchScore('wc', 'web-clients')).not.toBeNull()
    expect(matchScore('pg', 'whereabouts-pjxfg')).toBeNull()
    expect(matchScore('lhm', 'longhorn-manager')).toBeNull()
    expect(matchScore('lom', 'longhorn-manager')).not.toBeNull()
    const order = ['postgres', 'demo/postgres', 'mypostgres', 'pg-o-s-t-g-r-e-s'].map((t) => matchScore('postgres', t)!)
    expect([...order].sort((a, b) => b - a)).toEqual(order)
  })

  it('ranks by label first, then by hint, and keeps everything for an empty query', () => {
    const items = [
      { label: 'web', hint: 'demo Deployment' },
      { label: 'pgbench', hint: 'demo Deployment' },
      { label: 'demo-tools', hint: 'test' },
    ]
    expect(rank(items, 'demo').map((i) => i.label)).toEqual(['demo-tools', 'web', 'pgbench'])
    expect(rank(items, 'pgb').map((i) => i.label)).toEqual(['pgbench'])
    expect(rank(items, '')).toHaveLength(3)
  })
})
