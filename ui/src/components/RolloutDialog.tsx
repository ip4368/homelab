import { useEffect, useMemo, useState } from 'react'
import { op } from '../api'
import type { Change } from '../gen/Change'
import type { Preview } from '../gen/Preview'
import type { Rollout } from '../gen/Rollout'
import { describeIssue } from '../netedit'
import { useStore } from '../store'
import { useAction } from './useAction'

export function changeLabel(c: Change): string {
  return 'Template' in c ? `template ${c.Template.id}` : `network ${c.Network.id}`
}

export interface Progress {
  /** 1-based batch being worked on; the batch count once done. */
  batch: number
  batches: number
  state: string
  active: boolean
}

/** Where a rollout stands, for display; `now` is sim ms. */
export function rolloutProgress(r: Rollout, now: number): Progress {
  const batches = Math.ceil(r.hosts.length / Math.max(1, r.batch_size))
  const state = r.state
  if (state === 'Done') return { batch: batches, batches, state: 'Done', active: false }
  if (state === 'Aborted') return { batch: Math.min(r.current_batch + 1, batches), batches, state: 'Aborted', active: false }
  const batch = Math.min(r.current_batch + 1, batches)
  if (state === 'Running') return { batch, batches, state: 'Running', active: true }
  if ('Soaking' in state) return { batch, batches, state: `Soaking, ${Math.max(0, Math.ceil((state.Soaking.until - now) / 1000))} s left`, active: true }
  return { batch, batches, state: `Paused: ${state.Paused.reason}`, active: true }
}

function hostList(hosts: string[]): string {
  return hosts.length <= 6 ? hosts.join(', ') : `${hosts.slice(0, 6).join(', ')} and ${hosts.length - 6} more`
}

function RolloutRow({ r, highlight }: { r: Rollout; highlight: boolean }) {
  const now = useStore((s) => s.sim.now)
  const paused = useStore((s) => s.sim.paused)
  const { error, busy, run } = useAction()
  const p = rolloutProgress(r, now)
  const isPaused = typeof r.state === 'object' && 'Paused' in r.state
  const send = (o: 'PauseRollout' | 'ResumeRollout' | 'AbortRollout') => run(() => op({ [o]: { id: r.id } } as Parameters<typeof op>[0]))
  return (
    <li className={`${p.active ? '' : 'resolved'}${highlight ? ' highlight' : ''}`}>
      <div>
        <strong>{changeLabel(r.change)}</strong> <span className="muted">{r.id}</span>
      </div>
      <div>
        Batch {p.batch} / {p.batches} ({r.hosts.length} hosts, {r.batch_size} per batch). <span className={isPaused ? 'warn' : p.state === 'Done' ? 'ok' : ''}>{p.state}</span>
      </div>
      {r.failed.length > 0 && <div className="bad">Failed: {hostList(r.failed)}</div>}
      {r.skipped.length > 0 && <div className="muted">Skipped (not ready): {hostList(r.skipped)}</div>}
      {p.active && (
        <div className="actions">
          {isPaused ? (
            <button disabled={paused || busy} onClick={() => send('ResumeRollout')}>
              Resume
            </button>
          ) : (
            <button disabled={paused || busy} onClick={() => send('PauseRollout')}>
              Pause
            </button>
          )}
          <button disabled={paused || busy} onClick={() => send('AbortRollout')} title="Gives the rollout up and takes the change back">
            Abort
          </button>
          {error && <span className="bad">{error}</span>}
        </div>
      )}
    </li>
  )
}

/** Active rollouts first, then the most recent finished ones. */
export function RolloutList({ highlight }: { highlight?: string | null }) {
  const rollouts = useStore((s) => s.view.rollouts)
  const sorted = useMemo(() => {
    const all = Object.values(rollouts).sort((a, b) => b.version - a.version)
    const live = (r: Rollout) => r.state !== 'Done' && r.state !== 'Aborted'
    return [...all.filter(live), ...all.filter((r) => !live(r)).slice(0, 5)]
  }, [rollouts])
  if (sorted.length === 0) return <p className="muted">No rollouts yet.</p>
  return (
    <ul className="rail-list rollouts">
      {sorted.map((r) => (
        <RolloutRow key={r.id} r={r} highlight={r.id === highlight} />
      ))}
    </ul>
  )
}

function PreviewView({ change, preview }: { change: Change; preview: Preview }) {
  const issueHosts = Object.entries(preview.issues)
  const isNetwork = 'Network' in change
  return (
    <div className="preview">
      <p>
        <strong>{preview.affected.length}</strong> hosts affected, <strong>{preview.overridden.length}</strong> with an override, <strong>{preview.skipped.length}</strong> skipped (not ready).
      </p>
      {isNetwork ? (
        <p className="muted">
          A network change is validated against every host at once and takes effect in the control plane when the rollout starts. The batches then re-push each host&apos;s configuration, one batch at a time.
        </p>
      ) : (
        <p className="muted">The template changes when the rollout starts; each batch of hosts then switches to it, soaking before the next.</p>
      )}
      {preview.overridden.length > 0 && <p className="muted">Overridden: {hostList(preview.overridden)}</p>}
      {issueHosts.length > 0 ? (
        <div>
          <strong className="bad">Validation finds issues on {issueHosts.length} hosts</strong>
          <ul className="fixes">
            {issueHosts.slice(0, 12).map(([host, issues]) => (
              <li key={host}>
                <code>{host}</code>: {issues.map(describeIssue).join('; ')}
              </li>
            ))}
            {issueHosts.length > 12 && <li className="muted">and {issueHosts.length - 12} more hosts</li>}
          </ul>
        </div>
      ) : (
        <p className="ok">Validation finds no issues.</p>
      )}
      {preview.switch_changes.length > 0 && (
        <div>
          <strong>Switch changes the issues call for</strong>
          <ul className="fixes">
            {preview.switch_changes.map((c) => (
              <li key={c}>{c}</li>
            ))}
          </ul>
        </div>
      )}
    </div>
  )
}

interface Props {
  /** The change to preview and start; null shows only the rollouts. */
  change: Change | null
  onClose(): void
}

/** Preview, then start, a template or network change; shows every rollout's progress. */
export function RolloutDialog({ change, onClose }: Props) {
  const paused = useStore((s) => s.sim.paused)
  const [preview, setPreview] = useState<Preview | null>(null)
  const [includeOverridden, setIncludeOverridden] = useState(false)
  const [batchPct, setBatchPct] = useState(10)
  const [soakS, setSoakS] = useState(30)
  const [started, setStarted] = useState<string | null>(null)
  const previewing = useAction()
  const starting = useAction()

  useEffect(() => {
    if (!change) return
    let live = true
    void previewing.run(async () => {
      const p = (await op({ PreviewChange: { change } })) as Preview
      if (live) setPreview(p)
    })
    return () => {
      live = false
    }
    // The preview is of the change as opened; `previewing.run` is stable.
  }, [change, previewing.run])

  const start = () =>
    change &&
    starting.run(async () => {
      const id = await op({ StartRollout: { change, include_overridden: includeOverridden, batch_pct: batchPct, soak_s: soakS } })
      setStarted(String(id))
    })
  const valid = Number.isInteger(batchPct) && batchPct >= 1 && batchPct <= 100 && Number.isInteger(soakS) && soakS >= 0
  const nothing = preview !== null && preview.affected.length === 0 && (!includeOverridden || preview.overridden.length === 0)

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" role="dialog" aria-label="rollout" onClick={(e) => e.stopPropagation()}>
        <div className="drawer-head">
          <h3>{change ? `Roll out ${changeLabel(change)}` : 'Rollouts'}</h3>
          <div className="spacer" />
          <button onClick={onClose} aria-label="close">
            ×
          </button>
        </div>
        {change && !started && (
          <>
            {previewing.error && <div className="bad">{previewing.error}</div>}
            {!preview && !previewing.error && <p className="muted">Previewing...</p>}
            {preview && <PreviewView change={change} preview={preview} />}
            <div className="form-grid wide">
              <label>Overridden</label>
              <label className="inline">
                <input type="checkbox" checked={includeOverridden} onChange={(e) => setIncludeOverridden(e.target.checked)} /> include overridden hosts (drops their override)
              </label>
              <label>Batch size</label>
              <span>
                <input type="number" min={1} max={100} value={batchPct} onChange={(e) => setBatchPct(Number(e.target.value))} style={{ width: 70 }} aria-label="batch percent" /> % of affected hosts
              </span>
              <label>Soak</label>
              <span>
                <input type="number" min={0} value={soakS} onChange={(e) => setSoakS(Number(e.target.value))} style={{ width: 70 }} aria-label="soak seconds" /> s after each batch
              </span>
            </div>
            <div className="actions">
              <button className="active" disabled={paused || starting.busy || !preview || !valid} onClick={start}>
                Start rollout
              </button>
              <button onClick={onClose}>Cancel</button>
              {nothing && <span className="muted">No host is affected; the change still applies.</span>}
              {paused && <span className="muted">Paused: start needs the simulation running.</span>}
              {starting.error && <span className="bad">{starting.error}</span>}
            </div>
          </>
        )}
        {started && <p className="ok">Rollout {started} started.</p>}
        <h4 className="modal-sub">Rollouts</h4>
        <RolloutList highlight={started} />
      </div>
    </div>
  )
}
