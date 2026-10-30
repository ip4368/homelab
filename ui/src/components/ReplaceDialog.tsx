import { useMemo, useState } from 'react'
import { op } from '../api'
import { ineligibleReason } from '../eligibility'
import type { CpInfo } from '../gen/CpInfo'
import type { HostObj } from '../gen/HostObj'
import { useStore } from '../store'
import { useAction } from './useAction'

export interface Replacement {
  dead: string
  with: string
}

/**
 * The spares that may take `dead`'s control-plane place, best first: Ready,
 * eligible hosts that are no voter; MS-01s first, then hosts on a rack
 * (access switch) no voter uses, then by id. Mirrors the leader's
 * `candidates`, whose first pick the `CpReplacementSuggested` alert names —
 * so the default here is the suggested spare. The leader may still refuse
 * one (a change is in progress) and says why.
 */
export function replaceChoices(cp: CpInfo, hosts: Record<string, HostObj>): string[] {
  const racks = new Set(cp.voters.map((v) => hosts[v]?.location).filter((l) => l !== undefined))
  return Object.values(hosts)
    .filter((h) => h.lifecycle === 'Ready' && !cp.voters.includes(h.id) && ineligibleReason(h.profile, 'ControlPlane') === null)
    .sort((a, b) => Number(a.profile.id !== 'Ms01') - Number(b.profile.id !== 'Ms01') || Number(racks.has(a.location)) - Number(racks.has(b.location)) || a.id.localeCompare(b.id))
    .map((h) => h.id)
}

export type StepState = 'done' | 'active' | 'pending'

export interface Step {
  label: string
  state: StepState
}

/**
 * The replacement's steps, in order, as the cluster's state shows them:
 * the spare joins first, and only then does the dead voter leave.
 */
export function replaceSteps({ dead, with: with_ }: Replacement, cp: CpInfo): Step[] {
  const raw: [string, boolean][] = [
    [`${with_} joins the control plane`, cp.voters.includes(with_)],
    [`${dead} leaves the control plane`, !cp.voters.includes(dead)],
  ]
  const last = raw.map(([, done]) => done).lastIndexOf(true)
  let active = false
  return raw.map(([label, done], i) => {
    if (done || i < last) return { label, state: 'done' }
    const state: StepState = active ? 'pending' : 'active'
    active = true
    return { label, state }
  })
}

const STEP_MARK: Record<StepState, string> = { done: '✓', active: '●', pending: '○' }

interface Props {
  /** The down voter to replace. */
  host: HostObj
  onClose(): void
}

/**
 * Replaces a dead voter's control-plane place with a spare, now: pick the
 * spare, confirm, then follow the replacement as it goes. The same
 * operation the `CpReplacementSuggested` alert carries, without waiting
 * for the automatic replacement.
 */
export function ReplaceDialog({ host, onClose }: Props) {
  const cp = useStore((s) => s.view.cp)
  const hosts = useStore((s) => s.view.hosts)
  const choices = useMemo(() => replaceChoices(cp, hosts), [cp, hosts])
  const [other, setOther] = useState('')
  // The replacement this dialog started, followed once sent.
  const [sent, setSent] = useState<Replacement | null>(null)
  const { error, busy, run } = useAction()

  const following = sent
  const with_ = choices.find((c) => c === other) ?? choices[0]

  const submit = () =>
    with_ &&
    run(async () => {
      const replacement = { dead: host.id, with: with_ }
      await op({ ReplaceCpMember: replacement })
      setSent(replacement)
    })

  let body: React.ReactNode
  if (following) {
    const steps = replaceSteps(following, cp)
    const finished = steps.every((s) => s.state === 'done')
    body = (
      <>
        <p>
          {finished ? (
            <span className="ok">
              {following.with} took {following.dead}'s place in the control plane.
            </span>
          ) : (
            <>
              Replacing <strong>{following.dead}</strong> with <strong>{following.with}</strong>.
            </>
          )}
        </p>
        {!finished && (
          <ol className="handoff-steps">
            {steps.map((s) => (
              <li key={s.label} className={`step-${s.state}`}>
                <span className="step-mark" aria-hidden>
                  {STEP_MARK[s.state]}
                </span>
                <span>{s.label}</span>
              </li>
            ))}
          </ol>
        )}
        <div className="actions">
          <button onClick={onClose}>Close</button>
          {!finished && <span className="muted">The replacement goes on with this closed.</span>}
          {error && <span className="bad">{error}</span>}
        </div>
      </>
    )
  } else if (!with_) {
    body = (
      <>
        <p className="muted">No Ready, control-plane-eligible host is free to take its place.</p>
        <div className="actions">
          <button onClick={onClose}>Close</button>
        </div>
      </>
    )
  } else {
    body = (
      <>
        <div className="form-grid wide">
          <label>Dead voter</label>
          <span>
            <strong>{host.id}</strong>
          </span>
          <label>Spare</label>
          <span>
            <select value={with_} onChange={(e) => setOther(e.target.value)} aria-label="replace with">
              {choices.map((c, i) => (
                <option key={c} value={c}>
                  {c}
                  {i === 0 ? ' (suggested)' : ''}
                </option>
              ))}
            </select>
          </span>
        </div>
        <ul className="handoff-notes">
          <li>
            {with_} joins as a learner first and is promoted once caught up; only then does {host.id} leave, so the control plane never runs
            a voter short.
          </li>
          <li className="warn">Only replace a host that is gone for good: if it comes back, it stays in the fleet as a regular host, not a voter.</li>
        </ul>
        <div className="actions">
          <button className="active" disabled={busy} onClick={submit}>
            Replace voter
          </button>
          <button onClick={onClose}>Cancel</button>
          {error && <span className="bad">{error}</span>}
        </div>
      </>
    )
  }

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal handoff-dialog" role="dialog" aria-label="replace control-plane voter" onClick={(e) => e.stopPropagation()}>
        <div className="drawer-head">
          <h3>Replace control-plane voter</h3>
          <div className="spacer" />
          <button onClick={onClose} aria-label="close">
            ×
          </button>
        </div>
        {body}
      </div>
    </div>
  )
}
