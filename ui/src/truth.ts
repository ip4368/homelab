import type { LabTruth } from './gen/LabTruth'
import type { HostObj } from './gen/HostObj'
import type { View } from './store'

export interface TruthDiffEntry {
  hostId: string
  believed: string
  actual: string
}

/** Lifecycles in which the control plane takes a host to be up and heartbeating. */
const BELIEVED_UP = new Set<HostObj['lifecycle']>(['Ready', 'Cordoned'])

/**
 * Where the control plane's view and the lab's truth disagree: hosts the view
 * takes to be up that are powered off or frozen, and hosts it has as NotReady
 * that are running (cut off, not dead).
 */
export function truthDiff(view: Pick<View, 'hosts'>, truth: Pick<LabTruth, 'vms'>): TruthDiffEntry[] {
  const out: TruthDiffEntry[] = []
  for (const t of truth.vms) {
    const h = view.hosts[t.id]
    if (!h) continue
    if (BELIEVED_UP.has(h.lifecycle) && t.power === 'Off') out.push({ hostId: t.id, believed: h.lifecycle, actual: 'powered off' })
    else if (BELIEVED_UP.has(h.lifecycle) && t.power === 'Paused') out.push({ hostId: t.id, believed: h.lifecycle, actual: 'frozen' })
    else if (h.lifecycle === 'NotReady' && t.power === 'Running') out.push({ hostId: t.id, believed: h.lifecycle, actual: 'running' })
  }
  return out
}
