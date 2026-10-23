import { describe, expect, it } from 'vitest'
import type { Rollout } from '../gen/Rollout'
import { changeLabel, rolloutProgress } from './RolloutDialog'

const rollout = (patch: Partial<Rollout>): Rollout => ({
  id: 'r1',
  change: { Network: { id: 'storage', purpose: 'Storage', vlan: 20, subnet: '10.20.0.0/22', mtu: 9000 } },
  hosts: Array.from({ length: 25 }, (_, i) => `h${i}`),
  batch_size: 3,
  current_batch: 0,
  state: 'Running',
  failed: [],
  skipped: [],
  previous: null,
  dropped_overrides: {},
  soak_ms: 30000,
  base_generation: {},
  version: 1,
  ...patch,
})

describe('rolloutProgress', () => {
  it('counts batches from the hosts and batch size', () => {
    expect(rolloutProgress(rollout({}), 0)).toEqual({ batch: 1, batches: 9, state: 'Running', active: true })
  })

  it('shows the soak countdown and the pause reason', () => {
    expect(rolloutProgress(rollout({ current_batch: 2, state: { Soaking: { until: 40_000 } } }), 31_500).state).toBe('Soaking, 9 s left')
    expect(rolloutProgress(rollout({ state: { Paused: { reason: 'h1 reverted' } } }), 0).state).toBe('Paused: h1 reverted')
  })

  it('finished rollouts are inactive; done shows every batch', () => {
    expect(rolloutProgress(rollout({ current_batch: 9, state: 'Done' }), 0)).toEqual({ batch: 9, batches: 9, state: 'Done', active: false })
    expect(rolloutProgress(rollout({ current_batch: 3, state: 'Aborted' }), 0)).toMatchObject({ batch: 4, active: false })
  })

  it('labels changes', () => {
    expect(changeLabel(rollout({}).change)).toBe('network storage')
  })
})
