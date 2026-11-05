import { Fragment, useState } from 'react'
import { age, bytesUsed, containerDigests, cpu, gib, mem, pdbWarning, podTone, podWhy, volumeTone, workloadEvents, workloadKey, workloadVolumes } from '../kube'
import { nav } from '../nav'
import { useStore } from '../store'
import { fetchWorkloadMetrics } from '../metrics'
import { Charts, WORKLOAD_CHARTS } from './Charts'
import { Card, DetailPane, type PaneTab } from './DetailPane'
import { EndpointLink, HealthBadge, Hosts, NetRate, Usage } from './KubeBits'
import { PodButtons } from './PodButtons'
import { ScaleControl, WorkloadPaneActions } from './WorkloadActions'
import { Containers, History, PodDetail, VmGuest } from './WorkloadContainers'
import { healthTone, HEALTH_LABELS } from '../kube'

/**
 * One workload in the detail pane, as Kubernetes reports it: an overview
 * (health and why, usage and an hour of charts, endpoints, volumes), its
 * pods, its containers (a VM's guest), rollout history and Warning events.
 */
export function WorkloadDetail({ id, tab, onTab, onClose }: { id: string; tab: string | null; onTab(t: string): void; onClose(): void }) {
  const kube = useStore((s) => s.kube)
  const now = useStore((s) => s.clock.now)
  const [open, setOpen] = useState<string | null>(null)
  const w = kube.workloads.find((x) => workloadKey(x) === id)
  if (!w) return null
  const owner = `${w.kind}/${w.name}`
  const pods = kube.pods.filter((p) => p.namespace === w.namespace && p.owner === owner)
  const volumes = workloadVolumes(w, kube.pods, kube.volumes)
  const events = workloadEvents(w, kube.events)
  const pdb = pdbWarning(w.pdb)
  const unwell = pods.filter((p) => podTone(p) === 'bad' || podTone(p) === 'warn').length

  const tabs: PaneTab[] = [
    { id: 'overview', label: 'Overview' },
    { id: 'pods', label: 'Pods', count: pods.length, tone: unwell > 0 ? 'warn' : undefined },
    w.vm ? { id: 'guest', label: 'Guest' } : { id: 'containers', label: 'Containers', count: w.containers.length },
    ...(w.revisions.length > 0 ? [{ id: 'history', label: 'History', count: w.revisions.length }] : []),
    { id: 'events', label: 'Events', count: events.length, tone: events.length > 0 ? ('warn' as const) : undefined },
  ]

  return (
    <DetailPane
      label={`workload ${w.namespace}/${w.name}`}
      title={w.name}
      status={<span className={`pill ${healthTone(w.health.state)}`}>{HEALTH_LABELS[w.health.state]}</span>}
      subtitle={
        <>
          {w.kind} in <strong>{w.namespace}</strong> · <ScaleControl w={w} always /> ready · {w.updated}/{w.desired} on the latest spec
        </>
      }
      actions={<WorkloadPaneActions w={w} />}
      tabs={tabs}
      tab={tab}
      onTab={onTab}
      onClose={onClose}
    >
      {(t) => {
        if (t === 'pods')
          return (
            <div className="table-wrap">
              <table className="grid-table">
                <thead>
                  <tr>
                    <th>Pod</th>
                    <th>Status</th>
                    <th className="num">CPU</th>
                    <th className="num">Memory</th>
                    <th className="num hide-narrow">Network</th>
                    <th />
                  </tr>
                </thead>
                <tbody>
                  {pods.map((p) => (
                    <Fragment key={p.name}>
                      <tr className={open === p.name ? 'selected' : undefined}>
                        <td title={`created ${age(p.created, now)} ago`}>
                          <button className="link expander" aria-expanded={open === p.name} onClick={() => setOpen(open === p.name ? null : p.name)}>
                            {open === p.name ? '▾' : '▸'} <span className="cell-title">{p.name}</span>
                          </button>
                          <div className="cell-sub">
                            on {p.node ? <Hosts hosts={[p.node]} onSelectHost={(h) => nav.host(h)} /> : 'no host yet'} · {age(p.created, now)}
                          </div>
                        </td>
                        <td>
                          <span className={`pill ${podTone(p)}`}>{p.status}</span>
                          {p.restarts > 0 && <span className="warn"> {p.restarts} restart{p.restarts === 1 ? '' : 's'}</span>}
                          {podWhy(p) && <div className="muted pod-why">{podWhy(p)}</div>}
                        </td>
                        <td className="num">
                          <Usage used={p.cpu_milli} limit={p.cpu_limit_milli} format={cpu} />
                        </td>
                        <td className="num">
                          <Usage used={p.memory_bytes} limit={p.memory_limit_bytes} format={mem} />
                        </td>
                        <td className="num muted hide-narrow">
                          <NetRate rx={p.rx_bps} tx={p.tx_bps} />
                        </td>
                        <td className="row-actions">
                          <PodButtons pod={p} />
                        </td>
                      </tr>
                      {open === p.name && (
                        <tr className="expanded">
                          <td colSpan={6}>
                            <PodDetail pod={p} />
                          </td>
                        </tr>
                      )}
                    </Fragment>
                  ))}
                  {pods.length === 0 && (
                    <tr>
                      <td colSpan={6} className="muted">
                        No pods.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          )
        if (t === 'containers') return <Containers namespace={w.namespace} containers={w.containers} digests={containerDigests(w, kube.pods)} volumes={kube.volumes} />
        if (t === 'guest' && w.vm) return <VmGuest vm={w.vm} namespace={w.namespace} volumes={kube.volumes} />
        if (t === 'history') return <History revisions={w.revisions} now={now} />
        if (t === 'events')
          return events.length === 0 ? (
            <p className="muted">No Warning events in the last hour.</p>
          ) : (
            <ul className="event-list">
              {events.map((e, n) => (
                <li key={n}>
                  <div>
                    <strong className="warn">{e.reason}</strong>
                    {e.count > 1 && <span className="muted"> ×{e.count}</span>} <span className="muted">{e.object}</span>
                    <span className="spacer" />
                    <span className="muted">{e.last ? `${age(e.last, now)} ago` : ''}</span>
                  </div>
                  <div className="event-message">{e.message}</div>
                </li>
              ))}
            </ul>
          )
        return (
          <div className="card-grid">
            <Card title="Health">
              <HealthBadge health={w.health} full />
              {w.conditions.map((c) => (
                <div key={c.kind} className="warn" title={c.message ?? undefined}>
                  {c.kind}: {c.status}
                  {c.reason && ` (${c.reason})`}
                  {c.message && <span className="muted"> {c.message}</span>}
                </div>
              ))}
              {pdb && <div className="warn">{pdb}</div>}
              {w.host_access.length > 0 && <div className="warn">Its pods use the {w.host_access.join(', ')}.</div>}
              {unwell > 0 && (
                <p>
                  <button className="link" onClick={() => onTab('pods')}>
                    {unwell} pod{unwell === 1 ? '' : 's'} not running well
                  </button>
                </p>
              )}
              {events.length > 0 && (
                <p>
                  <button className="link warn" onClick={() => onTab('events')}>
                    {events.length} Warning event{events.length === 1 ? '' : 's'} in the last hour
                  </button>
                </p>
              )}
            </Card>
            <Card title="Usage now">
              {kube.metrics_error ? (
                <span className="muted">unknown: {kube.metrics_error}</span>
              ) : (
                <div className="kpis">
                  <div className="kpi">
                    <span className="kpi-value">
                      <Usage used={w.cpu_milli} limit={w.cpu_limit_milli} format={cpu} />
                    </span>
                    <span className="kpi-label">CPU</span>
                  </div>
                  <div className="kpi">
                    <span className="kpi-value">
                      <Usage used={w.memory_bytes} limit={w.memory_limit_bytes} format={mem} />
                    </span>
                    <span className="kpi-label">memory</span>
                  </div>
                  <div className="kpi">
                    <span className="kpi-value small-value">
                      <NetRate rx={w.rx_bps} tx={w.tx_bps} />
                    </span>
                    <span className="kpi-label">network</span>
                  </div>
                </div>
              )}
              {kube.stats_error && <div className="muted">{kube.stats_error}</div>}
            </Card>
            <Card title="Endpoints">
              {w.endpoints.length === 0 ? (
                <span className="muted">none: no Service selects its pods</span>
              ) : (
                <ul className="plain">
                  {w.endpoints.map((e) => (
                    <li key={e.address}>
                      <EndpointLink e={e} /> <span className="muted">{e.via}</span>
                    </li>
                  ))}
                </ul>
              )}
            </Card>
            <Card title="Volumes">
              {volumes.length === 0 ? (
                <span className="muted">No Longhorn volumes.</span>
              ) : (
                <ul className="plain">
                  {volumes.map((v) => (
                    <li key={v.name}>
                      {v.pvc ?? v.name} <span className="muted">{v.used_bytes === null ? gib(v.size_bytes) : `${bytesUsed(v.used_bytes, v.size_bytes)} used`}</span>{' '}
                      <span className={volumeTone(v)}>{v.robustness === 'unknown' ? v.state : v.robustness}</span>{' '}
                      <span className="muted">
                        ({v.replicas.filter((r) => r.mode === 'RW').length}/{v.replicas_wanted} replicas in sync)
                      </span>
                    </li>
                  ))}
                </ul>
              )}
            </Card>
            <Card title="Last hour" className="span-all">
              <Charts entity={id} title="" charts={WORKLOAD_CHARTS} fetchOnce={() => fetchWorkloadMetrics(w.namespace, w.kind, w.name)} everyMs={15_000} />
            </Card>
          </div>
        )
      }}
    </DetailPane>
  )
}
