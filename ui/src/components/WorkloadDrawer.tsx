import { Fragment, useState } from 'react'
import { age, bytesUsed, containerDigests, cpu, gib, mem, netRate, pdbWarning, podTone, podWhy, volumeTone, workloadEvents, workloadKey, workloadVolumes, canShell } from '../kube'
import { CONSOLE_LOG_CONTAINER, consoleSession, dock, logsSession, terminalSession } from '../dock'
import { useStore } from '../store'
import { EndpointLink, HealthBadge, Hosts, NetRate, Usage } from './KubeBits'
import { Containers, History, PodDetail, VmGuest } from './WorkloadContainers'
import { Charts, WORKLOAD_CHARTS } from './Charts'
import { fetchWorkloadMetrics } from '../metrics'

/**
 * One workload in full, as Kubernetes reports it: health and why, usage,
 * endpoints, containers, its pods, volumes, rollout history and Warning
 * events of the last hour.
 */
export function WorkloadDrawer({ id, onClose, onSelectHost }: { id: string; onClose(): void; onSelectHost(id: string): void }) {
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

  return (
    <aside className="drawer workload-drawer" aria-label={`workload ${w.namespace}/${w.name}`}>
      <div className="drawer-head">
        <h3>{w.name}</h3>
        <span className="muted">
          {w.kind} in {w.namespace}
        </span>
        <div className="spacer" />
        <button onClick={onClose} aria-label="close">
          ×
        </button>
      </div>

      <h4>Health</h4>
      <HealthBadge health={w.health} full />
      <div className="muted">
        {w.ready}/{w.desired} ready, {w.updated}/{w.desired} on the latest spec
      </div>
      {w.conditions.map((c) => (
        <div key={c.kind} className="warn" title={c.message ?? undefined}>
          {c.kind}: {c.status}
          {c.reason && ` (${c.reason})`}
          {c.message && <span className="muted"> {c.message}</span>}
        </div>
      ))}
      {pdb && <div className="warn">{pdb}</div>}
      {w.host_access.length > 0 && <div className="warn">Its pods use the {w.host_access.join(', ')}.</div>}

      <h4>Usage</h4>
      {kube.metrics_error ? (
        <span className="muted">unknown: {kube.metrics_error}</span>
      ) : (
        <div className="form-grid">
          <label>CPU</label>
          <Usage used={w.cpu_milli} limit={w.cpu_limit_milli} format={cpu} />
          <label>Memory</label>
          <Usage used={w.memory_bytes} limit={w.memory_limit_bytes} format={mem} />
          <label>Network</label>
          <span>{netRate(w.rx_bps, w.tx_bps)}</span>
        </div>
      )}
      {kube.stats_error && <div className="muted">{kube.stats_error}</div>}
      <Charts
        entity={id}
        title="Last hour"
        charts={WORKLOAD_CHARTS}
        fetchOnce={() => fetchWorkloadMetrics(w.namespace, w.kind, w.name)}
        everyMs={15_000}
      />

      <h4>Endpoints</h4>
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

      {w.vm ? (
        <>
          <h4>Guest</h4>
          <div className="actions-row">
            <button disabled={w.ready === 0} onClick={() => dock.open(consoleSession(w.namespace, w.name))} title="its serial console, as virtctl console">
              Console
            </button>{' '}
            {pods[0] && (
              <button onClick={() => dock.open(logsSession(pods[0], CONSOLE_LOG_CONTAINER))} title="what its serial console printed, even with nobody attached">
                Console log
              </button>
            )}
          </div>
          <VmGuest vm={w.vm} namespace={w.namespace} volumes={kube.volumes} />
        </>
      ) : (
        <>
          <h4>Containers</h4>
          <Containers namespace={w.namespace} containers={w.containers} digests={containerDigests(w, kube.pods)} volumes={kube.volumes} />
        </>
      )}

      <h4>Pods</h4>
      <div className="table-scroll">
        <table className="data pod-table">
          <thead>
            <tr>
              <th>Pod</th>
              <th>Status</th>
              <th>Usage</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {pods.map((p) => (
              <Fragment key={p.name}>
                <tr>
                  <td title={`created ${age(p.created, now)} ago`}>
                    <button className="link pod-name" aria-expanded={open === p.name} onClick={() => setOpen(open === p.name ? null : p.name)}>
                      {open === p.name ? '▾' : '▸'} {p.name}
                    </button>
                    <div className="muted">on {p.node ? <Hosts hosts={[p.node]} onSelectHost={onSelectHost} /> : 'no host yet'}</div>
                  </td>
                  <td className={podTone(p)} title={podWhy(p)}>
                    {p.status}
                    {p.restarts > 0 && <div className="warn">{p.restarts} restart{p.restarts === 1 ? '' : 's'}</div>}
                    {podWhy(p) && <div className="muted pod-why">{podWhy(p)}</div>}
                  </td>
                  <td className="num pod-usage">
                    <div>
                      <span className="muted">CPU </span>
                      <Usage used={p.cpu_milli} limit={p.cpu_limit_milli} format={cpu} />
                    </div>
                    <div>
                      <span className="muted">mem </span>
                      <Usage used={p.memory_bytes} limit={p.memory_limit_bytes} format={mem} />
                    </div>
                    <div className="muted">
                      <NetRate rx={p.rx_bps} tx={p.tx_bps} />
                    </div>
                  </td>
                  <td className="pod-actions">
                    <button onClick={() => dock.open(logsSession(p))}>Logs</button>
                    {canShell(p) && <button onClick={() => dock.open(terminalSession(p))}>Shell</button>}
                  </td>
                </tr>
                {open === p.name && (
                  <tr>
                    <td colSpan={4}>
                      <PodDetail pod={p} />
                    </td>
                  </tr>
                )}
              </Fragment>
            ))}
            {pods.length === 0 && (
              <tr>
                <td colSpan={4} className="muted">
                  No pods.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {volumes.length > 0 && (
        <>
          <h4>Volumes</h4>
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
        </>
      )}

      {w.revisions.length > 0 && (
        <>
          <h4>History</h4>
          <History revisions={w.revisions} now={now} />
        </>
      )}

      <h4>Warning events</h4>
      {events.length === 0 ? (
        <span className="muted">none in the last hour</span>
      ) : (
        <ul className="plain kube-events">
          {events.map((e, n) => (
            <li key={n}>
              <span className="muted">{e.last ? `${age(e.last, now)} ago` : ''}</span> <strong className="warn">{e.reason}</strong>
              {e.count > 1 && <span className="muted"> ×{e.count}</span>} <span className="muted">{e.object}</span>
              <div>{e.message}</div>
            </li>
          ))}
        </ul>
      )}
    </aside>
  )
}
