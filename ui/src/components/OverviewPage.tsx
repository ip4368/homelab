import { useMemo } from 'react'
import { attention } from '../attention'
import { formatClock } from '../clock'
import { volumeTone } from '../kube'
import { nav } from '../nav'
import { useStore } from '../store'
import { AlertAction, Events } from './ActivityPanel'
import { Charts } from './Charts'
import { Card } from './DetailPane'
import { lifecycleTone } from './HostDetail'
import { quorumOf } from './TopBar'

/** A number that answers one question about the cluster, and opens where to look. */
function Stat({ label, value, detail, tone, onClick }: { label: string; value: string; detail: string; tone: 'ok' | 'warn' | 'bad' | 'muted'; onClick?(): void }) {
  const body = (
    <>
      <span className="stat-label">{label}</span>
      <span className={`stat-value ${tone}`}>{value}</span>
      <span className="stat-detail">{detail}</span>
    </>
  )
  return onClick ? (
    <button className={`stat-card tone-${tone}`} onClick={onClick}>
      {body}
    </button>
  ) : (
    <div className={`stat-card tone-${tone}`}>{body}</div>
  )
}

/**
 * The first page: is anything wrong, and where. The cluster's health in a
 * row of numbers, what needs attention (each opens its detail), the fleet's
 * charts, the hosts at a glance and the latest events.
 */
export function OverviewPage() {
  const hosts = useStore((s) => s.view.hosts)
  const alerts = useStore((s) => s.view.alerts)
  const cp = useStore((s) => s.view.cp)
  const leader = useStore((s) => s.view.leader)
  const term = useStore((s) => s.view.term)
  const metrics = useStore((s) => s.view.metrics)
  const kube = useStore((s) => s.kube)
  const items = useMemo(() => attention(hosts, alerts, kube), [hosts, alerts, kube])

  const all = Object.values(hosts).filter((h) => h.lifecycle !== 'Removed')
  const ready = all.filter((h) => h.lifecycle === 'Ready').length
  const down = all.filter((h) => h.lifecycle === 'NotReady').length
  const quorum = quorumOf(cp, hosts)
  const healthy = kube.workloads.filter((w) => w.health.state === 'Healthy' || w.health.state === 'Idle').length
  const wDown = kube.workloads.filter((w) => w.health.state === 'Down').length
  const running = kube.pods.filter((p) => p.status === 'Running' || p.status === 'Succeeded' || p.status === 'Completed').length
  const restarts = kube.pods.reduce((n, p) => n + p.restarts, 0)
  const volOk = kube.volumes.filter((v) => volumeTone(v) === 'ok' || volumeTone(v) === 'muted').length
  const volBad = kube.volumes.some((v) => volumeTone(v) === 'bad')
  const sorted = [...all].sort((a, b) => a.id.localeCompare(b.id, 'en', { numeric: true }))

  return (
    <section className="overview page-grid">
      <div className="stats span-all">
        <Stat
          label="Hosts"
          value={`${ready}/${all.length}`}
          detail={down > 0 ? `${down} NotReady` : all.length === ready ? 'all Ready' : `${all.length - ready} not Ready`}
          tone={down > 0 ? 'bad' : ready < all.length ? 'warn' : all.length ? 'ok' : 'muted'}
          onClick={() => nav.page('hosts')}
        />
        <Stat
          label="Control plane"
          value={`${quorum.live}/${quorum.voters}`}
          detail={leader ? `${leader} leads, term ${term}` : 'no leader'}
          tone={quorum.voters === 0 ? 'muted' : quorum.ok && leader ? 'ok' : 'bad'}
          onClick={leader ? () => nav.host(leader) : undefined}
        />
        <Stat
          label="Workloads"
          value={kube.at > 0 ? `${healthy}/${kube.workloads.length}` : '–'}
          detail={kube.at === 0 ? 'waiting for Kubernetes' : healthy === kube.workloads.length ? 'all healthy' : `${kube.workloads.length - healthy} not healthy`}
          tone={kube.at === 0 ? 'muted' : wDown > 0 ? 'bad' : healthy < kube.workloads.length ? 'warn' : 'ok'}
          onClick={() => nav.page('workloads')}
        />
        <Stat
          label="Pods"
          value={kube.at > 0 ? `${running}/${kube.pods.length}` : '–'}
          detail={`${restarts} restart${restarts === 1 ? '' : 's'} in all`}
          tone={kube.at === 0 ? 'muted' : running < kube.pods.length ? 'warn' : 'ok'}
          onClick={() => nav.page('workloads', 'pods')}
        />
        <Stat
          label="Volumes"
          value={kube.at > 0 ? `${volOk}/${kube.volumes.length}` : '–'}
          detail="Longhorn, healthy or detached"
          tone={kube.at === 0 ? 'muted' : volBad ? 'bad' : volOk < kube.volumes.length ? 'warn' : 'ok'}
          onClick={() => nav.page('workloads', 'volumes')}
        />
      </div>

      <Card title="Needs attention" extra={<span className={items.length ? 'warn' : 'ok'}>{items.length ? `${items.length} item${items.length === 1 ? '' : 's'}` : 'nothing'}</span>} className="attention-card">
        {items.length === 0 ? (
          <p className="all-clear">
            <span className="dot ok" /> All hosts Ready, every workload and volume healthy, no open alerts.
          </p>
        ) : (
          <ul className="attention">
            {items.map((i) => (
              <li key={i.key} className={`tone-${i.tone}`}>
                <span className={`dot ${i.tone}`} aria-hidden />
                <div className="attention-body">
                  {i.open ? (
                    <button className="link attention-title" onClick={() => i.open && nav.open(i.open.kind, i.open.id)}>
                      {i.title}
                    </button>
                  ) : (
                    <strong className="attention-title">{i.title}</strong>
                  )}
                  <div className="muted">{i.detail}</div>
                  {i.alert?.action && <AlertAction action={i.alert.action} />}
                </div>
                {i.alert && <span className="muted when">{formatClock(i.alert.raised_at)}</span>}
              </li>
            ))}
          </ul>
        )}
      </Card>

      <Card title="Hosts" extra={<button className="link" onClick={() => nav.page('hosts')}>all hosts</button>}>
        <ul className="host-strip">
          {sorted.map((h) => {
            const m = metrics[h.id]
            return (
              <li key={h.id}>
                <button className="host-row" onClick={() => nav.host(h.id)}>
                  <span className={`dot ${lifecycleTone(h.lifecycle)}`} aria-hidden />
                  <span className="host-name">{h.id}</span>
                  {leader === h.id && <span className="leader-mark">VIP</span>}
                  <span className="spacer" />
                  {h.lifecycle !== 'Ready' && <span className={`pill ${lifecycleTone(h.lifecycle)}`}>{h.lifecycle}</span>}
                  <span className="mini-bars" title={m ? `CPU ${m.cpu_pct.toFixed(0)}%, RAM ${m.ram_pct.toFixed(0)}%` : 'no sample yet'}>
                    <span className="mini-bar">
                      <span style={{ width: `${m?.cpu_pct ?? 0}%` }} />
                    </span>
                    <span className="mini-bar ram">
                      <span style={{ width: `${m?.ram_pct ?? 0}%` }} />
                    </span>
                  </span>
                </button>
              </li>
            )
          })}
          {sorted.length === 0 && <li className="muted">No hosts yet.</li>}
        </ul>
        <p className="muted legend">
          Bars: <span className="mini-bar inline">CPU</span> and <span className="mini-bar ram inline">RAM</span>
        </p>
      </Card>

      <Card title="Fleet" className="span-all">
        <Charts entity="fleet" title="" row />
        <p className="muted">Average CPU and RAM over the hosts, and their total port throughput.</p>
      </Card>

      <Card title="Latest events" className="span-all events-card">
        <Events limit={12} />
      </Card>
    </section>
  )
}
