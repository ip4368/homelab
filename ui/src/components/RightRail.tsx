import { useMemo, useState } from 'react'
import { op } from '../api'
import type { HostObj } from '../gen/HostObj'
import type { Role } from '../gen/Role'
import { useStore } from '../store'
import { formatClock } from './TopBar'
import { RolePicker } from './RolePicker'
import { useAction } from './useAction'

type Tab = 'Alerts' | 'Events' | 'Pending joins'
const TABS: Tab[] = ['Alerts', 'Events', 'Pending joins']
const RESOLVED_SHOWN = 20
const EVENTS_SHOWN = 200

function Alerts({ onSelect }: { onSelect(id: string): void }) {
  const alerts = useStore((s) => s.view.alerts)
  const { active, resolved } = useMemo(() => {
    const all = Object.values(alerts).sort((a, b) => b.raised_at - a.raised_at)
    return {
      active: all.filter((a) => a.resolved_at === null),
      resolved: all.filter((a) => a.resolved_at !== null).slice(0, RESOLVED_SHOWN),
    }
  }, [alerts])
  if (active.length + resolved.length === 0) return <p className="muted">No alerts.</p>
  return (
    <ul className="rail-list">
      {[...active, ...resolved].map((a) => (
        <li key={a.id} className={a.resolved_at === null ? '' : 'resolved'}>
          <span className={`sev sev-${a.severity}`}>{a.severity}</span> <strong>{a.kind}</strong>{' '}
          {a.entity.kind === 'Host' ? (
            <button className="link" onClick={() => onSelect(a.entity.id)}>
              {a.entity.id}
            </button>
          ) : (
            <span className="muted">{a.entity.id}</span>
          )}
          <div>{a.message}</div>
          <div className="muted">
            {formatClock(a.raised_at)}
            {a.resolved_at !== null && ` — resolved ${formatClock(a.resolved_at)}`}
          </div>
        </li>
      ))}
    </ul>
  )
}

/** VMs the simulator sees running on several hosts at once. The control plane cannot see this, so it is shown only with the ground-truth overlay. */
function SplitBrain() {
  const alerts = useStore((s) => s.truth.alerts)
  const items = useMemo(
    () => alerts.filter((a) => a.kind === 'VmSplitBrain').sort((a, b) => (a.resolved_at === null ? 0 : 1) - (b.resolved_at === null ? 0 : 1) || b.raised_at - a.raised_at),
    [alerts],
  )
  return (
    <div className="truth-section">
      <h4>
        Split-brain <span className="truth-label">ground truth</span>
      </h4>
      {items.length === 0 ? (
        <p className="muted">No VM is running in two places.</p>
      ) : (
        <ul className="rail-list">
          {items.map((a) => (
            <li key={a.id} className={a.resolved_at === null ? '' : 'resolved'}>
              <span className={`sev sev-${a.severity}`}>{a.severity}</span> <strong>{a.kind}</strong> <span className="muted">{a.entity.id}</span>
              <div>{a.message}</div>
              <div className="muted">
                {formatClock(a.raised_at)}
                {a.resolved_at !== null && ` — resolved ${formatClock(a.resolved_at)}`}
              </div>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}

function Events() {
  const events = useStore((s) => s.view.events)
  const shown = useMemo(() => events.slice(-EVENTS_SHOWN).reverse(), [events])
  if (shown.length === 0) return <p className="muted">No events.</p>
  return (
    <ul className="rail-list">
      {shown.map((e) => (
        <li key={e.seq}>
          <span className="muted">{formatClock(e.at)}</span> <strong>{e.entity.id}</strong>
          <div>{e.message}</div>
        </li>
      ))}
    </ul>
  )
}

function PendingRow({ host, disabled, onSelect }: { host: HostObj; disabled: boolean; onSelect(id: string): void }) {
  const [open, setOpen] = useState(false)
  const [roles, setRoles] = useState<Role[]>(['Compute'])
  const { error, busy, run } = useAction()
  return (
    <li>
      <button className="link" onClick={() => onSelect(host.id)}>
        {host.id}
      </button>{' '}
      <span className="muted">{host.profile.id}</span>
      {open ? (
        <div>
          <RolePicker profile={host.profile} value={roles} onChange={setRoles} eligibleOnly />
          <div className="actions">
            <button
              disabled={disabled || busy || roles.length === 0}
              onClick={() => run(() => op({ ApproveHost: { id: host.id, roles } }))}
            >
              Confirm approve
            </button>
            <button onClick={() => setOpen(false)}>Cancel</button>
          </div>
        </div>
      ) : (
        <div className="actions">
          <button disabled={disabled} onClick={() => setOpen(true)}>
            Approve…
          </button>
        </div>
      )}
      {error && <div className="bad">{error}</div>}
    </li>
  )
}

function PendingJoins({ onSelect }: { onSelect(id: string): void }) {
  const hosts = useStore((s) => s.view.hosts)
  const tokens = useStore((s) => s.view.tokens)
  const paused = useStore((s) => s.sim.paused)
  const now = useStore((s) => s.sim.now)
  const pending = useMemo(
    () => Object.values(hosts).filter((h) => h.lifecycle === 'Pending').sort((a, b) => a.id.localeCompare(b.id)),
    [hosts],
  )
  const { error, busy, run } = useAction()
  const [created, setCreated] = useState<string | null>(null)
  const liveTokens = Object.values(tokens).filter((t) => t.expires_at > now)

  return (
    <div>
      <div className="actions">
        <button
          disabled={paused || busy}
          onClick={() =>
            run(async () => {
              setCreated(String(await op('CreateJoinToken')))
            })
          }
        >
          Create join token
        </button>
        {error && <span className="bad">{error}</span>}
      </div>
      {created && (
        <div>
          New token: <code>{created}</code>
        </div>
      )}
      {liveTokens.length > 0 && <p className="muted">{liveTokens.length} unexpired join token(s)</p>}
      {pending.length === 0 ? (
        <p className="muted">No hosts waiting to join.</p>
      ) : (
        <ul className="rail-list">
          {pending.map((h) => (
            <PendingRow key={h.id} host={h} disabled={paused} onSelect={onSelect} />
          ))}
        </ul>
      )}
    </div>
  )
}

export function RightRail({ onSelect, showTruth }: { onSelect(id: string): void; showTruth: boolean }) {
  const [tab, setTab] = useState<Tab>('Alerts')
  const activeAlerts = useStore((s) => s.view.alerts)
  const hosts = useStore((s) => s.view.hosts)
  const counts = useMemo(
    () => ({
      Alerts: Object.values(activeAlerts).filter((a) => a.resolved_at === null).length,
      Events: 0,
      'Pending joins': Object.values(hosts).filter((h) => h.lifecycle === 'Pending').length,
    }),
    [activeAlerts, hosts],
  )
  return (
    <aside className="rail">
      <div className="tabs" role="tablist">
        {TABS.map((t) => (
          <button key={t} role="tab" aria-selected={t === tab} className={t === tab ? 'active' : ''} onClick={() => setTab(t)}>
            {t}
            {counts[t] > 0 && ` (${counts[t]})`}
          </button>
        ))}
      </div>
      <div className="rail-body">
        {tab === 'Alerts' && showTruth && <SplitBrain />}
        {tab === 'Alerts' && <Alerts onSelect={onSelect} />}
        {tab === 'Events' && <Events />}
        {tab === 'Pending joins' && <PendingJoins onSelect={onSelect} />}
      </div>
    </aside>
  )
}
