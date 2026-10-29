import { useEffect, useMemo, useState } from 'react'
import { op } from '../api'
import { formatClock } from '../clock'
import { ineligibleReason } from '../eligibility'
import type { CpInfo } from '../gen/CpInfo'
import type { Event as ClusterEvent } from '../gen/Event'
import type { HostObj } from '../gen/HostObj'
import { useStore } from '../store'
import { useAction } from './useAction'

export interface Handoff {
  from: string
  to: string
}

/**
 * The hand-offs offered for `host`'s control-plane place: a voter hands its
 * place to any Ready, eligible host that is no voter; such a host takes any
 * voter's. The leader may still refuse one (a change is in progress) and says why.
 */
export function handoffChoices(host: HostObj, cp: CpInfo, hosts: Record<string, HostObj>): Handoff[] {
  const mayJoin = (h: HostObj) => h.lifecycle === 'Ready' && !cp.voters.includes(h.id) && ineligibleReason(h.profile, 'ControlPlane') === null
  if (cp.voters.includes(host.id)) {
    return Object.values(hosts)
      .filter(mayJoin)
      .map((h) => ({ from: host.id, to: h.id }))
      .sort((a, b) => a.to.localeCompare(b.to))
  }
  return mayJoin(host) ? [...cp.voters].sort().map((v) => ({ from: v, to: host.id })) : []
}

export type StepState = 'done' | 'active' | 'pending'

export interface Step {
  label: string
  state: StepState
  /** What the host is doing, for the active step. */
  detail: string | null
}

function runs(h: HostObj | undefined, role: 'Server' | 'Agent'): boolean {
  return h?.kube?.role === role && h.kube.phase === 'Running'
}

/** What `h`'s K3s is doing, if it is not simply running. */
function kubeDoing(h: HostObj | undefined): string | null {
  const k = h?.kube
  if (!k) return null
  if (typeof k.phase === 'object' && 'Failed' in k.phase) return `failed: ${k.phase.Failed}; retrying`
  if (k.phase === 'Installing') return k.detail ?? 'installing'
  return null
}

/**
 * The hand-off's steps, in order, as the cluster's state shows them: each
 * is done once what it leads to holds, and the first that is not is active.
 */
export function handoffSteps({ from, to }: Handoff, cp: CpInfo, hosts: Record<string, HostObj>): Step[] {
  const raw: [string, boolean, string | null][] = [
    [`${to} joins the control plane`, cp.voters.includes(to), null],
    [`${to} runs a K3s server`, runs(hosts[to], 'Server'), kubeDoing(hosts[to])],
    [`${from} leaves the control plane`, !cp.voters.includes(from), null],
    [`${from} runs a K3s agent`, runs(hosts[from], 'Agent'), kubeDoing(hosts[from])],
  ]
  // A later step that holds makes the earlier ones done too: once `from` has
  // left, `to` running a server is no longer what the hand-off waits on.
  const last = raw.map(([, done]) => done).lastIndexOf(true)
  let active = false
  return raw.map(([label, done], i) => {
    if (done || i < last) return { label, state: 'done', detail: null }
    const state: StepState = active ? 'pending' : 'active'
    active = true
    return { label, state, detail: state === 'active' ? raw[i][2] : null }
  })
}

/**
 * Why the hand-off ended without its old voter leaving, from the newest
 * control-plane event that says so: given up by the control plane, or
 * cancelled by the user.
 */
export function handoffEndReason({ from, to }: Handoff, events: ClusterEvent[]): string | undefined {
  const place = `handing ${from}'s control-plane place to ${to}`
  const ends = [`gave up ${place}`, `stopped ${place}`]
  return [...events].reverse().find((e) => e.entity.kind === 'Cp' && ends.some((m) => e.message.startsWith(m)))?.message
}

const STEP_MARK: Record<StepState, string> = { done: '✓', active: '●', pending: '○' }

function Steps({ steps }: { steps: Step[] }) {
  return (
    <ol className="handoff-steps">
      {steps.map((s) => (
        <li key={s.label} className={`step-${s.state}`}>
          <span className="step-mark" aria-hidden>
            {STEP_MARK[s.state]}
          </span>
          <span>
            {s.label}
            {s.detail && <div className="muted">{s.detail}</div>}
          </span>
        </li>
      ))}
    </ol>
  )
}

function kubeRole(h: HostObj | undefined): string {
  return h?.kube?.role ? `K3s ${h.kube.role.toLowerCase()}` : 'no K3s yet'
}

/** The two hosts, what each runs now and after the hand-off. */
function Plan({ handoff, hosts }: { handoff: Handoff; hosts: Record<string, HostObj> }) {
  const row = (id: string, now: string, after: string) => (
    <tr key={id}>
      <td>
        <strong>{id}</strong> <span className="muted">{hosts[id]?.profile.id}</span>
      </td>
      <td>{now}</td>
      <td>→</td>
      <td>{after}</td>
    </tr>
  )
  return (
    <table className="handoff-plan">
      <thead>
        <tr>
          <th>Host</th>
          <th>Now</th>
          <th />
          <th>After</th>
        </tr>
      </thead>
      <tbody>
        {row(handoff.from, `voter, ${kubeRole(hosts[handoff.from])}`, 'not a voter, K3s agent')}
        {row(handoff.to, `not a voter, ${kubeRole(hosts[handoff.to])}`, 'voter, K3s server (API + etcd)')}
      </tbody>
    </table>
  )
}

interface Props {
  /** The host whose drawer opened the dialog. */
  host: HostObj
  onClose(): void
}

/**
 * Hands a voter's control-plane place to another host: pick the other end,
 * review what changes, confirm, then follow the hand-off as it goes.
 */
export function HandoffDialog({ host, onClose }: Props) {
  const cp = useStore((s) => s.view.cp)
  const hosts = useStore((s) => s.view.hosts)
  const leader = useStore((s) => s.view.leader)
  const events = useStore((s) => s.view.events)
  const choices = useMemo(() => handoffChoices(host, cp, hosts), [host, cp, hosts])
  const voter = cp.voters.includes(host.id)
  // The other end: the host to hand to, or the voter to take over from.
  const [other, setOther] = useState('')
  // The hand-off this dialog started, followed after it ends.
  const [sent, setSent] = useState<Handoff | null>(null)
  // Whether the view has shown a hand-off in progress since the dialog opened:
  // until then, one this dialog sent has not reached the view yet.
  const [seen, setSeen] = useState(cp.handoff !== null)
  useEffect(() => {
    if (cp.handoff) setSeen(true)
  }, [cp.handoff])
  const { error, busy, run } = useAction()

  const following: Handoff | null = cp.handoff ?? sent ?? null
  const choice = choices.find((c) => (voter ? c.to : c.from) === other) ?? choices[0]
  const down = cp.voters.filter((v) => hosts[v]?.lifecycle === 'NotReady')

  const submit = () =>
    choice &&
    run(async () => {
      await op({ HandOffCp: choice })
      setSent(choice)
    })
  const cancel = () => run(() => op('CancelCpHandoff'))

  let body: React.ReactNode
  if (following) {
    const steps = handoffSteps(following, cp, hosts)
    const finished = steps.every((s) => s.state === 'done')
    // Ended without `from` leaving: given up or cancelled, and its event says why.
    const ended = seen && !cp.handoff && cp.voters.includes(following.from)
    const why = ended ? handoffEndReason(following, events) : undefined
    const cancelling = cp.handoff?.cancelled ?? false
    // Cancellable until `from` has left; after that only the end's event is left to record.
    const cancellable = cp.handoff !== null && !cancelling && cp.voters.includes(following.from)
    body = (
      <>
        <p>
          {!seen ? (
            <span className="muted">Starting the hand-off…</span>
          ) : cancelling ? (
            <span className="warn">
              Cancelling: {following.to} leaves the control plane again, if it joined, and {following.from} stays.
            </span>
          ) : cp.handoff ? (
            <>
              Handing <strong>{following.from}</strong>'s place to <strong>{following.to}</strong>, since {formatClock(cp.handoff.since)}.
            </>
          ) : ended ? (
            <span className="bad">{why ? `The control plane ${why}.` : 'The hand-off was given up.'}</span>
          ) : finished ? (
            <span className="ok">
              {following.to} took {following.from}'s place in the control plane.
            </span>
          ) : (
            <>
              {following.from} left the control plane; it is being reinstalled as a K3s agent.
            </>
          )}
        </p>
        {!cancelling && !ended && <Steps steps={steps} />}
        {cancellable && (
          <p className="muted">
            Stuck? Cancelling takes {following.to} out of the control plane again (it reinstalls as a K3s agent); {following.from} keeps its place.
          </p>
        )}
        <div className="actions">
          <button onClick={onClose}>Close</button>
          {cancellable && (
            <button disabled={busy} onClick={cancel}>
              Cancel hand-off
            </button>
          )}
          {cp.handoff && <span className="muted">The hand-off goes on with this closed.</span>}
          {error && <span className="bad">{error}</span>}
        </div>
      </>
    )
  } else if (!choice) {
    body = (
      <>
        <p className="muted">
          {voter ? 'No Ready, control-plane-eligible host is free to take its place.' : `${host.id} cannot join the control plane.`}
        </p>
        <div className="actions">
          <button onClick={onClose}>Close</button>
        </div>
      </>
    )
  } else {
    const fixed = voter ? choice.from : choice.to
    const select = (
      <select value={voter ? choice.to : choice.from} onChange={(e) => setOther(e.target.value)} aria-label={voter ? 'hand off to' : 'take over from'}>
        {choices.map((c) => {
          const id = voter ? c.to : c.from
          return (
            <option key={id} value={id}>
              {id}
              {!voter && id === leader ? ' (leader)' : ''}
            </option>
          )
        })}
      </select>
    )
    body = (
      <>
        <div className="form-grid wide">
          <label>From</label>
          <span>{voter ? <strong>{fixed}</strong> : select}</span>
          <label>To</label>
          <span>{voter ? select : <strong>{fixed}</strong>}</span>
        </div>
        <Plan handoff={choice} hosts={hosts} />
        <h4 className="modal-sub">What happens</h4>
        <Steps steps={handoffSteps(choice, cp, hosts).map((s) => ({ ...s, state: 'pending', detail: null }))} />
        <ul className="handoff-notes">
          <li>
            {choice.from} leaves only once {choice.to}'s K3s server runs, so the control plane and etcd never run a member short. There are{' '}
            {cp.voters.length + 1} voters for a while.
          </li>
          {choice.from === leader && (
            <li className="warn">
              {choice.from} leads the control plane. It steps down as it leaves, and the VIP (this UI's API, the Kubernetes API) moves to the next leader:
              expect a few seconds without answers.
            </li>
          )}
          {down.length > 0 && <li className="bad">Voter {down.join(', ')} is down: the control plane refuses a hand-off until it is replaced.</li>}
        </ul>
        <div className="actions">
          <button className="active" disabled={busy || down.length > 0} onClick={submit}>
            Hand off control plane
          </button>
          <button onClick={onClose}>Cancel</button>
          {error && <span className="bad">{error}</span>}
        </div>
      </>
    )
  }

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal handoff-dialog" role="dialog" aria-label="hand off control plane" onClick={(e) => e.stopPropagation()}>
        <div className="drawer-head">
          <h3>Hand off control plane</h3>
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
