import { useMemo, useState } from 'react'
import { op } from '../api'
import type { CpInfo } from '../gen/CpInfo'
import type { HostObj } from '../gen/HostObj'
import { useStore } from '../store'
import { canJoin, downVoters, isDown, kubeDoing, runs, Steps, toSteps, type Step } from './HandoffDialog'
import { useAction } from './useAction'

export interface Replacement {
  dead: string
  with: string
}

/**
 * The replacements offered from `host`'s drawer: a down voter is replaced by
 * any host that may join (MS-01s first, as the control plane suggests), and
 * such a host may replace any down voter. The leader may still refuse one (a
 * change is in progress) and says why.
 */
export function replaceChoices(host: HostObj, cp: CpInfo, hosts: Record<string, HostObj>): Replacement[] {
  if (cp.voters.includes(host.id)) {
    if (!isDown(host)) return []
    const ms01First = (a: HostObj, b: HostObj) => Number(b.profile.id === 'Ms01') - Number(a.profile.id === 'Ms01') || a.id.localeCompare(b.id)
    return Object.values(hosts)
      .filter((h) => canJoin(h, cp))
      .sort(ms01First)
      .map((h) => ({ dead: host.id, with: h.id }))
  }
  return canJoin(host, cp) ? downVoters(cp, hosts).map((d) => ({ dead: d, with: host.id })) : []
}

/** The replacement's steps, as the cluster's state shows them. */
export function replaceSteps(r: Replacement, cp: CpInfo, hosts: Record<string, HostObj>): Step[] {
  return toSteps([
    [`${r.with} joins the control plane`, cp.voters.includes(r.with), null],
    [`${r.dead} leaves the control plane (and etcd)`, !cp.voters.includes(r.dead), null],
    [`${r.with} runs a K3s server`, runs(hosts[r.with], 'Server'), kubeDoing(hosts[r.with])],
  ])
}

interface Props {
  /** The host whose drawer opened the dialog: the down voter, or a host to replace one with. */
  host: HostObj
  onClose(): void
}

/** Replaces a down voter with a host that may join: pick, confirm, then follow it. */
export function ReplaceDialog({ host, onClose }: Props) {
  const cp = useStore((s) => s.view.cp)
  const hosts = useStore((s) => s.view.hosts)
  const choices = useMemo(() => replaceChoices(host, cp, hosts), [host, cp, hosts])
  const voter = cp.voters.includes(host.id)
  const [other, setOther] = useState('')
  const [sent, setSent] = useState<Replacement | null>(null)
  const { error, busy, run } = useAction()
  const choice = choices.find((c) => (voter ? c.with : c.dead) === other) ?? choices[0]

  let body: React.ReactNode
  if (sent) {
    const steps = replaceSteps(sent, cp, hosts)
    body = (
      <>
        <p>
          {steps.every((s) => s.state === 'done') ? (
            <span className="ok">
              {sent.with} took {sent.dead}'s place in the control plane.
            </span>
          ) : (
            <>
              Replacing <strong>{sent.dead}</strong> with <strong>{sent.with}</strong>.
            </>
          )}
        </p>
        <Steps steps={steps} />
        <div className="actions">
          <button onClick={onClose}>Close</button>
          <span className="muted">The replacement goes on with this closed.</span>
        </div>
      </>
    )
  } else if (!choice) {
    body = (
      <>
        <p className="muted">No Ready, control-plane-eligible host is free to take its place.</p>
        <div className="actions">
          <button onClick={onClose}>Close</button>
        </div>
      </>
    )
  } else {
    const select = (
      <select value={voter ? choice.with : choice.dead} onChange={(e) => setOther(e.target.value)} aria-label={voter ? 'replace with' : 'replace'}>
        {choices.map((c) => {
          const id = voter ? c.with : c.dead
          return (
            <option key={id} value={id}>
              {id}
            </option>
          )
        })}
      </select>
    )
    const submit = () =>
      run(async () => {
        await op({ ReplaceCpMember: choice })
        setSent(choice)
      })
    body = (
      <>
        <div className="form-grid wide">
          <label>Down voter</label>
          <span>{voter ? <strong>{choice.dead}</strong> : select}</span>
          <label>Replace with</label>
          <span>{voter ? select : <strong>{choice.with}</strong>}</span>
        </div>
        <h4 className="modal-sub">What happens</h4>
        <Steps steps={replaceSteps(choice, cp, hosts).map((s) => ({ ...s, state: 'pending', detail: null }))} />
        <ul className="handoff-notes">
          <li>
            {choice.with} joins the control plane, then {choice.dead} is removed from it and from etcd. Only then does {choice.with} install a K3s server, so etcd never
            counts a dead member while it joins.
          </li>
          <li>
            If {choice.dead} comes back afterwards, it reinstalls K3s as an agent. If it is gone for good, remove it from the fleet with <strong>Remove</strong>.
          </li>
        </ul>
        <div className="actions">
          <button className="active" disabled={busy} onClick={submit}>
            Replace {choice.dead}
          </button>
          <button onClick={onClose}>Cancel</button>
          {error && <span className="bad">{error}</span>}
        </div>
      </>
    )
  }

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal handoff-dialog" role="dialog" aria-label="replace a down voter" onClick={(e) => e.stopPropagation()}>
        <div className="drawer-head">
          <h3>Replace a down voter</h3>
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
