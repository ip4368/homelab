import { useMemo, type ReactNode } from 'react'
import { formatClock } from '../clock'
import { age, byteRate, cpu, gib, mem, usedPct, workloadKey } from '../kube'
import { summarizeOverview, type OverviewTarget } from '../overview'
import { useStore } from '../store'
import { Charts } from './Charts'
import { HealthBadge } from './KubeBits'
import { quorumOf } from './TopBar'

export type OverviewView = 'fleet' | 'topology' | 'networks' | 'workloads'

interface Props {
  onNavigate(view: OverviewView): void
  onSelectHost(id: string): void
  onSelectWorkload(key: string): void
  onSelectSwitch(id: string): void
  onOpenActivity(): void
}

function Card({ title, action, children, className = '' }: { title: string; action?: ReactNode; children: ReactNode; className?: string }) {
  return (
    <article className={`ops-card ${className}`}>
      <header className="ops-card-head">
        <h3>{title}</h3>
        {action}
      </header>
      {children}
    </article>
  )
}

function Distribution({ parts }: { parts: { label: string; value: number; tone: 'ok' | 'warn' | 'bad' | 'muted' | 'accent' }[] }) {
  const total = parts.reduce((n, p) => n + p.value, 0)
  return (
    <div className="distribution-wrap">
      <div className="distribution" aria-hidden>
        {total === 0 ? <span style={{ width: '100%' }} className="dist-muted" /> : parts.filter((p) => p.value > 0).map((p) => <span key={p.label} className={`dist-${p.tone}`} style={{ width: `${(p.value / total) * 100}%` }} />)}
      </div>
      <div className="distribution-legend">
        {parts.map((p) => (
          <span key={p.label}>
            <i className={`dot dot-${p.tone}`} /> {p.label} <strong>{p.value}</strong>
          </span>
        ))}
      </div>
    </div>
  )
}

function pct(v: number | null): string {
  return v === null ? '—' : `${v.toFixed(0)}%`
}

/** The landscape home screen: what needs attention, capacity, fleet and workloads at once. */
export function Overview({ onNavigate, onSelectHost, onSelectWorkload, onSelectSwitch, onOpenActivity }: Props) {
  const view = useStore((s) => s.view)
  const kube = useStore((s) => s.kube)
  const truth = useStore((s) => s.truth)
  const clock = useStore((s) => s.clock)
  const connection = useStore((s) => s.connection)
  const summary = useMemo(
    () => summarizeOverview({ hosts: Object.values(view.hosts), metrics: view.metrics, alerts: Object.values(view.alerts), switches: Object.values(view.switches), kube, truth }),
    [view.hosts, view.metrics, view.alerts, view.switches, kube, truth],
  )
  const quorum = quorumOf(view.cp, view.hosts)
  const claims = useMemo(
    () => [...kube.claims].sort((a, b) => (usedPct(b.used_bytes, b.capacity_bytes ?? b.requested_bytes) ?? -1) - (usedPct(a.used_bytes, a.capacity_bytes ?? a.requested_bytes) ?? -1)).slice(0, 5),
    [kube.claims],
  )
  const openTarget = (target: OverviewTarget) => {
    if (target.kind === 'host') onSelectHost(target.id)
    else if (target.kind === 'workload') onSelectWorkload(target.key)
    else if (target.kind === 'switch') onSelectSwitch(target.id)
    else if (target.kind === 'activity') onOpenActivity()
    else onNavigate(target.kind)
  }
  const recent = [...view.events].slice(-8).reverse()
  const kubeWarnings = kube.events.slice(0, 5)

  return (
    <section className="overview" aria-label="Operations overview">
      <div className="page-heading overview-heading">
        <div>
          <span className="eyebrow">Mission control</span>
          <h2>Operations overview</h2>
          <p className="muted">
            {connection === 'live' ? 'Live cluster view' : connection === 'connecting' ? 'Connecting…' : 'Control plane unavailable'} · {formatClock(clock.now)}
            {kube.at > 0 && ` · Kubernetes read ${age(kube.at, clock.now)} ago`}
          </p>
        </div>
        <div className="heading-actions">
          <button onClick={() => onNavigate('fleet')}>Open fleet</button>
          <button className="active" onClick={() => onNavigate('workloads')}>
            Open workloads
          </button>
        </div>
      </div>

      <div className="kpi-grid" aria-label="Key cluster indicators">
        <button className="kpi" onClick={() => onNavigate('fleet')}>
          <span>Hosts ready</span>
          <strong>
            {summary.hosts.ready}<small>/{summary.hosts.total}</small>
          </strong>
          <em className={summary.hosts.notReady ? 'bad' : 'muted'}>{summary.hosts.notReady} not ready · {summary.hosts.pending} pending</em>
        </button>
        <button className="kpi" onClick={() => onNavigate('workloads')}>
          <span>Workloads healthy</span>
          <strong>
            {summary.workloads.healthy}<small>/{summary.workloads.total}</small>
          </strong>
          <em className={summary.workloads.down || summary.workloads.degraded ? 'bad' : 'muted'}>{summary.workloads.down} down · {summary.workloads.degraded} degraded</em>
        </button>
        <button className="kpi" onClick={() => onNavigate('workloads')}>
          <span>Pods ready</span>
          <strong>
            {summary.workloads.readyPods}<small>/{summary.workloads.pods}</small>
          </strong>
          <em className={summary.workloads.restartingPods ? 'warn' : 'muted'}>{summary.workloads.restartingPods} with restarts</em>
        </button>
        <button className="kpi" onClick={() => onNavigate('fleet')}>
          <span>Fleet load</span>
          <strong>{pct(summary.hosts.avgCpu)} <small>CPU</small></strong>
          <em>{pct(summary.hosts.avgRam)} RAM · {summary.hosts.sampled} sampled</em>
        </button>
        <button className="kpi" onClick={() => onNavigate('workloads')}>
          <span>Workload network</span>
          <strong className="kpi-long">↓ {byteRate(summary.workloads.netRxBps)}</strong>
          <em>↑ {byteRate(summary.workloads.netTxBps)}</em>
        </button>
        <button className={`kpi ${summary.attention.some((a) => a.severity === 'bad') ? 'kpi-alert' : ''}`} onClick={onOpenActivity}>
          <span>Needs attention</span>
          <strong>{summary.attention.length}</strong>
          <em className={summary.alerts.critical ? 'bad' : 'muted'}>{summary.alerts.critical} critical · {summary.alerts.warning} warning alerts</em>
        </button>
      </div>

      <div className="overview-grid">
        <Card title="Needs attention" className="ops-attention" action={<button className="link" onClick={onOpenActivity}>All activity</button>}>
          {summary.attention.length === 0 ? (
            <div className="healthy-state">
              <strong className="ok">All clear</strong>
              <span className="muted">No active alerts, unhealthy workloads, node pressure, or degraded volumes.</span>
            </div>
          ) : (
            <div className="attention-list">
              {summary.attention.map((a) => (
                <button key={a.id} className="attention-item" onClick={() => openTarget(a.target)}>
                  <i className={`severity-dot ${a.severity}`} />
                  <span>
                    <strong>{a.title}</strong>
                    <small>{a.detail}</small>
                  </span>
                  <span aria-hidden>›</span>
                </button>
              ))}
            </div>
          )}
        </Card>

        <Card title="Fleet capacity" className="ops-capacity" action={<button className="link" onClick={() => onNavigate('fleet')}>Fleet</button>}>
          <Charts entity="fleet" title="Average CPU and RAM · total port throughput" row />
        </Card>

        <Card title="Fleet" className="ops-fleet" action={<button className="link" onClick={() => onNavigate('fleet')}>View all</button>}>
          <Distribution
            parts={[
              { label: 'Ready', value: summary.hosts.ready, tone: 'ok' },
              { label: 'NotReady', value: summary.hosts.notReady, tone: 'bad' },
              { label: 'Pending', value: summary.hosts.pending, tone: 'warn' },
              { label: 'Other', value: summary.hosts.other, tone: 'muted' },
            ]}
          />
          <div className="mini-table" role="table" aria-label="Busiest hosts">
            {summary.hosts.busiest.map((h) => (
              <button key={h.id} onClick={() => onSelectHost(h.id)}>
                <span><strong>{h.id}</strong><small>{h.lifecycle}</small></span>
                <span className="load-pair"><i style={{ width: `${h.cpu ?? 0}%` }} /><b>CPU {pct(h.cpu)}</b></span>
                <span className="load-pair"><i style={{ width: `${h.ram ?? 0}%` }} /><b>RAM {pct(h.ram)}</b></span>
              </button>
            ))}
            {summary.hosts.busiest.length === 0 && <p className="muted">No hosts yet.</p>}
          </div>
        </Card>

        <Card title="Workloads" className="ops-workloads" action={<button className="link" onClick={() => onNavigate('workloads')}>View all</button>}>
          <Distribution
            parts={[
              { label: 'Healthy', value: summary.workloads.healthy, tone: 'ok' },
              { label: 'Rolling', value: summary.workloads.rolling, tone: 'accent' },
              { label: 'Degraded', value: summary.workloads.degraded, tone: 'warn' },
              { label: 'Down', value: summary.workloads.down, tone: 'bad' },
              { label: 'Idle', value: summary.workloads.idle, tone: 'muted' },
            ]}
          />
          <div className="workload-mini-list">
            {summary.workloads.attentionFirst.map((w) => (
              <button key={workloadKey(w)} onClick={() => onSelectWorkload(workloadKey(w))}>
                <span className="workload-mini-name">
                  <strong>{w.name}</strong>
                  <small>{w.namespace} · {w.kind}</small>
                </span>
                <HealthBadge health={w.health} />
                <span className="num">{cpu(w.cpu_milli)}</span>
                <span className="num">{mem(w.memory_bytes)}</span>
                <span className="num muted">↓{byteRate(w.net_rx_bps)}</span>
              </button>
            ))}
            {summary.workloads.total === 0 && <p className="muted">No workloads read yet.</p>}
          </div>
          <div className="ops-footnote muted">
            {summary.workloads.pods} pods · {cpu(summary.workloads.cpuMilli)} CPU · {mem(summary.workloads.memoryBytes)} memory{summary.workloads.rolling ? ` · ${summary.workloads.rolling} rolling out` : ''}
          </div>
        </Card>

        <Card title="Control & infrastructure" className="ops-infra" action={<button className="link" onClick={() => onNavigate('topology')}>Topology</button>}>
          <dl className="fact-list">
            <div><dt>Leader</dt><dd>{view.leader ?? 'none'} <span className="muted">term {view.term}</span></dd></div>
            <div><dt>Quorum</dt><dd className={quorum.ok ? 'ok' : 'bad'}>{quorum.live}/{quorum.voters} voters live</dd></div>
            <div><dt>Kubernetes nodes</dt><dd>{summary.infrastructure.readyKubeNodes}/{summary.infrastructure.kubeNodes} ready</dd></div>
            <div><dt>Switches</dt><dd>{summary.infrastructure.switches} observed</dd></div>
            <div><dt>Lab machines</dt><dd>{summary.infrastructure.labVmsRunning}/{summary.infrastructure.labVms} running</dd></div>
            <div><dt>Partitions</dt><dd className={summary.infrastructure.partitions ? 'warn' : ''}>{summary.infrastructure.partitions} active</dd></div>
            <div><dt>VIP</dt><dd>{summary.infrastructure.vipHolders.join(', ') || 'no holder reported'}</dd></div>
          </dl>
          {summary.infrastructure.pressures.length > 0 && (
            <div className="compact-list">
              {summary.infrastructure.pressures.map((p) => (
                <button key={`${p.node}-${p.pressure}`} className="link warn" onClick={() => onSelectHost(p.node)}>
                  {p.node}: {p.pressure}
                </button>
              ))}
            </div>
          )}
        </Card>

        <Card title="Storage" className="ops-storage" action={<button className="link" onClick={() => onNavigate('workloads')}>Volumes</button>}>
          <div className="storage-summary">
            <span><strong>{summary.storage.volumes}</strong> Longhorn volumes</span>
            <span className={summary.storage.faulted ? 'bad' : summary.storage.degraded ? 'warn' : 'ok'}>{summary.storage.faulted} faulted · {summary.storage.degraded} degraded</span>
            <span className="muted">{summary.storage.claims} claims · {summary.storage.pendingClaims} not bound · {summary.storage.detached} detached</span>
          </div>
          <div className="claim-list">
            {claims.map((c) => {
              const percent = usedPct(c.used_bytes, c.capacity_bytes ?? c.requested_bytes)
              return (
                <div key={`${c.namespace}/${c.name}`} className="claim-row">
                  <span><strong>{c.name}</strong><small>{c.namespace} · {c.phase}</small></span>
                  <span className="claim-bar"><i style={{ width: `${percent ?? 0}%` }} /></span>
                  <b>{percent === null ? '—' : `${percent}%`}</b>
                  <small>{c.used_bytes === null ? 'use unknown' : `${mem(c.used_bytes)} / ${gib(c.capacity_bytes ?? c.requested_bytes ?? 0)}`}</small>
                </div>
              )
            })}
            {claims.length === 0 && <p className="muted">No claims reported.</p>}
          </div>
        </Card>

        <Card title="Recent activity" className="ops-activity" action={<button className="link" onClick={onOpenActivity}>Open activity</button>}>
          <div className="activity-columns">
            <div>
              <h4>Control plane</h4>
              {recent.length === 0 ? <p className="muted">No events.</p> : recent.map((e) => (
                <div key={e.seq} className="activity-row"><time>{formatClock(e.at)}</time><span><strong>{e.entity.id}</strong><small>{e.message}</small></span></div>
              ))}
            </div>
            <div>
              <h4>Kubernetes warnings</h4>
              {kubeWarnings.length === 0 ? <p className="muted">None in the last hour.</p> : kubeWarnings.map((e, n) => (
                <div key={n} className="activity-row"><time>{e.last ? age(e.last, clock.now) : ''}</time><span><strong>{e.reason}</strong><small>{e.object} · {e.message}</small></span></div>
              ))}
            </div>
          </div>
        </Card>
      </div>
    </section>
  )
}
