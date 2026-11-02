import { age, cpu, gib, mem, podTone, podWhy, volumeTone, workloadEvents, workloadKey, workloadVolumes } from '../kube'
import { useStore } from '../store'
import { EndpointLink, HealthBadge, Hosts, Usage } from './KubeBits'

/**
 * One workload in full, as Kubernetes reports it: health and why, usage,
 * endpoints, images, its pods, volumes and Warning events of the last hour.
 */
export function WorkloadDrawer({ id, onClose, onSelectHost }: { id: string; onClose(): void; onSelectHost(id: string): void }) {
  const kube = useStore((s) => s.kube)
  const now = useStore((s) => s.clock.now)
  const w = kube.workloads.find((x) => workloadKey(x) === id)
  if (!w) return null
  const owner = `${w.kind}/${w.name}`
  const pods = kube.pods.filter((p) => p.namespace === w.namespace && p.owner === owner)
  const volumes = workloadVolumes(w, kube.pods, kube.volumes)
  const events = workloadEvents(w, kube.events)

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

      <h4>Usage</h4>
      {kube.metrics_error ? (
        <span className="muted">unknown: {kube.metrics_error}</span>
      ) : (
        <div className="form-grid">
          <label>CPU</label>
          <Usage used={w.cpu_milli} limit={w.cpu_limit_milli} format={cpu} />
          <label>Memory</label>
          <Usage used={w.memory_bytes} limit={w.memory_limit_bytes} format={mem} />
        </div>
      )}

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

      <h4>Images</h4>
      <ul className="plain">
        {w.images.map((i) => (
          <li key={i}>
            <code>{i}</code>
          </li>
        ))}
      </ul>

      <h4>Pods</h4>
      <table className="data">
        <thead>
          <tr>
            <th>Name</th>
            <th>Host</th>
            <th>Status</th>
            <th>Restarts</th>
            <th>CPU</th>
            <th>Memory</th>
          </tr>
        </thead>
        <tbody>
          {pods.map((p) => (
            <tr key={p.name}>
              <td title={`created ${age(p.created, now)} ago`}>{p.name}</td>
              <td>{p.node ? <Hosts hosts={[p.node]} onSelectHost={onSelectHost} /> : <span className="muted">none</span>}</td>
              <td className={podTone(p)} title={podWhy(p)}>
                {p.status}
                {podWhy(p) && <div className="muted pod-why">{podWhy(p)}</div>}
              </td>
              <td className={p.restarts > 0 ? 'warn' : 'muted'}>{p.restarts}</td>
              <td className="num">
                <Usage used={p.cpu_milli} limit={p.cpu_limit_milli} format={cpu} />
              </td>
              <td className="num">
                <Usage used={p.memory_bytes} limit={p.memory_limit_bytes} format={mem} />
              </td>
            </tr>
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

      {volumes.length > 0 && (
        <>
          <h4>Volumes</h4>
          <ul className="plain">
            {volumes.map((v) => (
              <li key={v.name}>
                {v.pvc ?? v.name} <span className="muted">{gib(v.size_bytes)}</span>{' '}
                <span className={volumeTone(v)}>{v.robustness === 'unknown' ? v.state : v.robustness}</span>{' '}
                <span className="muted">
                  ({v.replicas.filter((r) => r.mode === 'RW').length}/{v.replicas_wanted} replicas in sync)
                </span>
              </li>
            ))}
          </ul>
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
