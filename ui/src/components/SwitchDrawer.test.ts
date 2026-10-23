import { describe, expect, it } from 'vitest'
import type { SwitchPortCfg } from '../gen/SwitchPortCfg'
import { cfgFromForm, describeMode, formFromCfg } from './SwitchDrawer'

const trunk: SwitchPortCfg = { mode: { Trunk: { native: 1, allowed: [1, 20] } }, mtu: 9000, lacp_group: 'g1', admin_up: true }

describe('port config form', () => {
  it('round-trips a config through the form', () => {
    for (const cfg of [trunk, { mode: { Access: 100 }, mtu: 1500, lacp_group: null, admin_up: false }, { ...trunk, mode: { Trunk: { native: null, allowed: [] } } }] as SwitchPortCfg[]) {
      expect(cfgFromForm(formFromCfg(cfg))).toEqual({ cfg })
    }
  })

  it('parses, dedupes and sorts the allowed VLANs and trims blanks to null', () => {
    const f = { ...formFromCfg(trunk), native: ' ', allowed: '30, 20 20  1', lacp: ' ' }
    expect(cfgFromForm(f)).toEqual({ cfg: { ...trunk, mode: { Trunk: { native: null, allowed: [1, 20, 30] } }, lacp_group: null } })
  })

  it('rejects bad VLANs and MTUs with a message naming the field', () => {
    const base = formFromCfg(trunk)
    expect(cfgFromForm({ ...base, native: '5000' })).toEqual({ error: 'Native VLAN: "5000" is not a VLAN id (1-4094)' })
    expect(cfgFromForm({ ...base, allowed: '1, x' })).toMatchObject({ error: expect.stringContaining('Allowed VLANs') })
    expect(cfgFromForm({ ...base, mode: 'Access', access: '0' })).toMatchObject({ error: expect.stringContaining('Access VLAN') })
    expect(cfgFromForm({ ...base, mtu: '100' })).toMatchObject({ error: expect.stringContaining('MTU') })
  })

  it('describes modes', () => {
    expect(describeMode({ Access: 1 })).toBe('access 1')
    expect(describeMode(trunk.mode)).toBe('trunk native 1 allow 1,20')
  })
})
