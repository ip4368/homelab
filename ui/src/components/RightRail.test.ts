import { describe, expect, it } from 'vitest'
import { alertAction } from './RightRail'

describe('alert actions', () => {
  it('names the replacement an alert suggests', () => {
    const a = alertAction({ ReplaceCpMember: { dead: 'ms01-02', with: 'minipc-01' } })
    expect(a.label).toBe('Replace with minipc-01')
    expect(a.confirm).toMatch(/^Replace control-plane voter ms01-02 with minipc-01\?/)
  })
})
