import { describe, expect, it } from 'vitest'
import { alertActionLabel } from './RightRail'

describe('alert remediation', () => {
  it('labels the replacement an alert offers', () => {
    expect(alertActionLabel({ ReplaceCpMember: { dead: 'v2', with: 'ms01-9' } })).toBe('Replace v2 with ms01-9')
    expect(alertActionLabel('CreateJoinToken')).toBe('Run')
  })
})
