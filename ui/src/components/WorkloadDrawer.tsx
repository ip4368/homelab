import { useState } from 'react'
import { execUrl, SHELLS } from '../debug'
import type { KubeClaim } from '../gen/KubeClaim'
import type { KubeNode } from '../gen/KubeNode'
import type { KubePod } from '../gen/KubePod'
import { age, byteRate, cpu, gib, mem, podClaims, podTone, podWhy, PROTECTED_NAMESPACES, usedPct, volumeTone, workloadEvents, workloadKey, workloadVolumes } from '../kube'
import { useStore } from '../store'
import { KubeCharts } from './Charts'
import { EndpointLink, HealthBadge, Hosts, Usage } from './KubeBits'
import { LogPanel } from './LogPanel'
import { Terminal } from './Terminal'

interface ShellTarget {
  pod: string
  container: string
  shell: string
}

/** A claim's fullness, when the kubelet reports it: `1.2Gi of 2Gi used (60%)`. */
function claimUse(c: KubeClaim): string | null {
  const pct = usedPct(c.used_bytes, c.capacity_bytes ?? c.requested_bytes)
  if (c.used_bytes === null || pct === null) return null
  return `${mem(c.used_bytes)} of ${mem(c.capacity_bytes ?? c.requested_bytes)} used (${pct}%)`
}

/** One pod in full: where it sits, its containers, mounts, history, logs, and a shell into each container. */
function PodDetail({ p, claims, nodes, canExec, onSelectHost, onShell }: { p: KubePod; claims: KubeClaim[]; nodes: KubeNode[]; canExec: boolean; onSelectHost(id: string): void; onShell(t: ShellTarget): void }) {
  const [shell, setShell] = useState<string>(SHELLS[0])
  const node = nodes.find((n) => n.name === p.node)
  const mine = podClaims(p, claims)
  const bySource = new Map(mine.map((c) => [c.name, c]))
  const meta: [string, string][] = [
    ['Pod IP', p.pod_ip ?? '–'],
    ['QoS', p.qos ?? '–'],
    ['Service account', p.service_account ?? 'default'],
    ['Ephemeral storage', p.fs_used_bytes === null ? '–' : mem(p.fs_used_bytes)],
    ['Network', p.net_rx_bps === null && p.net_tx_bps === null ? '–' : `↓ ${byteRate(p.net_rx_bps)} ↑ ${byteRate(p.net_tx_bps)}`],
  ]
  if (p.init_containers.length > 0) meta.push(['Init containers', p.init_containers.join(', ')])
  const mounts = p.container_details.flatMap((c) => [
    ...c.mounts.map((m) => ({ ...m, device: false, container: c.name })),
    ...c.devices.map((m) => ({ ...m, device: true, container: c.name })),
  ])

  return (
    <div className="pod-detail">
      <div className="pod-heading">
        <div>
          <span className="eyebrow">Selected pod</span>
          <h4>{p.name}</h4>
        </div>
        <span className={`status-pill ${podTone(p)}`}>{p.status}</span>
      </div>
      <div className="fact-grid">
        {meta.map(([k, v]) => (
          <span key={k} className="fact-chip">
            <small>{k}</small>
            <b>{v}</b>
          </span>
        ))}
      </div>
      {node && (
        <div className="muted pod-node-line">
          On {p.node ? <Hosts hosts={[p.node]} onSelectHost={onSelectHost} /> : 'no host'}: {node.ready ? 'Ready' : 'not Ready'}
          {node.cpu_allocatable_milli !== null && p.cpu_request_milli !== null && `, requests ${usedPct(p.cpu_request_milli, node.cpu_allocatable_milli)}% of its allocatable CPU`}
          {node.memory_allocatable_bytes !== null && p.memory_request_bytes !== null && ` and ${usedPct(p.memory_request_bytes, node.memory_allocatable_bytes)}% of its memory`}
          {node.kubelet_version && ` · kubelet ${node.kubelet_version}`}
          {node.pressures.length > 0 && <span className="warn"> · under {node.pressures.join(', ')}</span>}
        </div>
      )}
      <h5>Containers</h5>
          <div className="table-scroll compact-scroll">
            <table className="data">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>State</th>
                  <th>CPU (req → limit)</th>
                  <th>Memory (req → limit)</th>
                  <th>Ports</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {p.container_details.map((c) => (
                  <tr key={c.name}>
                    <td title={c.image}>
                      <strong>{c.name}</strong> <span className="muted">{c.image}</span>
                      {Object.keys(c.extra_requests).length > 0 && <div className="muted">{Object.entries(c.extra_requests).map(([k, v]) => `${k}: ${v}`).join(', ')}</div>}
                      {c.probes.length > 0 && <div className="muted">probes: {c.probes.join(', ')}</div>}
                    </td>
                    <td>
                      {c.state}
                      {c.restarts > 0 && <div className="warn">{c.restarts} restarts{c.last_termination ? `, last ${c.last_termination.reason} (exit ${c.last_termination.exit_code})` : ''}</div>}
                    </td>
                    <td className="num">
                      <Usage used={c.cpu_milli} limit={c.cpu_limit_milli} format={cpu} />
                      <div className="muted">req {cpu(c.cpu_request_milli)}</div>
                    </td>
                    <td className="num">
                      <Usage used={c.memory_bytes} limit={c.memory_limit_bytes} format={mem} />
                      <div className="muted">req {mem(c.memory_request_bytes)}</div>
                    </td>
                    <td className="muted">{c.ports.map((x) => `${x.name ? `${x.name}:` : ''}${x.port}/${x.protocol}${x.host_port ? ` (host ${x.host_port})` : ''}`).join(', ') || '–'}</td>
                    <td>
                      {canExec && (
                        <button disabled={!c.ready && c.state !== 'Running'} title={`a shell in ${c.name}`} onClick={() => onShell({ pod: p.name, container: c.name, shell })}>
                          Shell
                        </button>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {canExec && (
            <label className="inline">
              Shell{' '}
              <select value={shell} onChange={(e) => setShell(e.target.value)} aria-label="shell">
                {SHELLS.map((s) => (
                  <option key={s}>{s}</option>
                ))}
              </select>
            </label>
          )}

      <h5>Mounts and devices</h5>
      {mounts.length === 0 ? <span className="muted">none</span> : (
          <div className="table-scroll compact-scroll">
            <table className="data">
              <thead><tr><th>Path</th><th>From</th><th /></tr></thead>
              <tbody>
                {mounts.map((m, i) => {
                  const claim = m.source.startsWith('pvc ') ? bySource.get(m.source.slice(4)) : undefined
                  const use = claim ? claimUse(claim) : null
                  return (
                    <tr key={i}>
                      <td><code>{m.path}</code> <span className="muted">in {m.container}{m.device ? ' (block device)' : ''}</span></td>
                      <td>{m.source}{m.sub_path && <span className="muted"> (subPath {m.sub_path})</span>}{use && <div className="muted">{use}</div>}</td>
                      <td className="muted">{m.read_only ? 'read-only' : ''}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
      )}
      <KubeCharts entity={{ pod: `${p.namespace}/${p.name}` }} title="Pod usage, last hour" />
      <h5>Logs</h5>
      <LogPanel namespace={p.namespace} pod={p.name} containers={p.container_details.map((c) => c.name)} restarts={p.restarts} />
    </div>
  )
}

/**
 * One workload in full, as Kubernetes reports it: health and why, usage and
 * requests, endpoints, images, its pods in full (containers, mounts,
 * history, logs, shells), volumes and Warning events of the last hour.
 */
export function WorkloadDrawer({ id, onClose, onSelectHost }: { id: string; onClose(): void; onSelectHost(id: string): void }) {
  const kube = useStore((s) => s.kube)
  const now = useStore((s) => s.clock.now)
  const [selected, setSelected] = useState<string | null>(null)
  const [shellTarget, setShellTarget] = useState<ShellTarget | null>(null)
  const w = kube.workloads.find((x) => workloadKey(x) === id)
  if (!w) return null
  const owner = `${w.kind}/${w.name}`
  const pods = kube.pods.filter((p) => p.namespace === w.namespace && p.owner === owner)
  const volumes = workloadVolumes(w, kube.pods, kube.volumes)
  const events = workloadEvents(w, kube.events)
  const pod = pods.find((p) => p.name === selected) ?? pods[0]
  const canExec = !PROTECTED_NAMESPACES.includes(w.namespace)

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

            <section className="inspector-section">
              <h4>Health</h4>
              <HealthBadge health={w.health} full />
              <div className="muted">{w.ready}/{w.desired} ready, {w.updated}/{w.desired} on the latest spec</div>
              {w.conditions.map((c) => (
                <div key={c.kind} className="warn" title={c.message ?? undefined}>
                  {c.kind}: {c.status}{c.reason && ` (${c.reason})`}{c.message && <span className="muted"> {c.message}</span>}
                </div>
              ))}
            </section>
            <section className="inspector-section">
              <h4>Usage and requests</h4>
              {kube.metrics_error ? <span className="muted">unknown: {kube.metrics_error}</span> : (
                <div className="metric-card-grid">
                  <div className="metric-card"><span>CPU</span><strong><Usage used={w.cpu_milli} limit={w.cpu_limit_milli} format={cpu} /></strong><small>requests {cpu(w.cpu_request_milli)}</small></div>
                  <div className="metric-card"><span>Memory</span><strong><Usage used={w.memory_bytes} limit={w.memory_limit_bytes} format={mem} /></strong><small>requests {mem(w.memory_request_bytes)}</small></div>
                  <div className="metric-card"><span>Network</span><strong>↓ {byteRate(w.net_rx_bps)}</strong><small>↑ {byteRate(w.net_tx_bps)}{kube.stats_error ? ` · ${kube.stats_error}` : ''}</small></div>
                </div>
              )}
              <KubeCharts entity={{ workload: `${w.namespace}/${w.kind}/${w.name}` }} title="Usage, last hour" />
            </section>
            <div className="inspector-two-col">
              <section className="inspector-section">
                <h4>Endpoints</h4>
                {w.endpoints.length === 0 ? <span className="muted">none: no Service selects its pods</span> : (
                  <ul className="plain">{w.endpoints.map((e) => <li key={e.address}><EndpointLink e={e} /> <span className="muted">{e.via}</span></li>)}</ul>
                )}
              </section>
              <section className="inspector-section">
                <h4>Images</h4>
                <ul className="plain">{w.images.map((i) => <li key={i}><code>{i}</code></li>)}</ul>
              </section>
            </div>

      <h4>Pods</h4>
            <div className="table-scroll pod-table-scroll">
              <table className="data">
                <thead><tr><th>Name</th><th>Host</th><th>Status</th><th>Restarts</th><th>CPU</th><th>Memory</th><th>Network</th></tr></thead>
                <tbody>
                  {pods.map((p) => (
                    <tr key={p.name} className={pod?.name === p.name ? 'selected-row' : undefined}>
                      <td title={`created ${age(p.created, now)} ago${p.pod_ip ? ` · ${p.pod_ip}` : ''}`}><button className="link" onClick={() => setSelected(p.name)} title="pod detail, logs and shells below">{p.name}</button></td>
                      <td>{p.node ? <Hosts hosts={[p.node]} onSelectHost={onSelectHost} /> : <span className="muted">none</span>}</td>
                      <td className={podTone(p)} title={podWhy(p)}>{p.status}{podWhy(p) && <div className="muted pod-why">{podWhy(p)}</div>}</td>
                      <td className={p.restarts > 0 ? 'warn' : 'muted'}>{p.restarts}</td>
                      <td className="num"><Usage used={p.cpu_milli} limit={p.cpu_limit_milli} format={cpu} /></td>
                      <td className="num"><Usage used={p.memory_bytes} limit={p.memory_limit_bytes} format={mem} /></td>
                      <td className="num muted">↓{byteRate(p.net_rx_bps)} ↑{byteRate(p.net_tx_bps)}</td>
                    </tr>
                  ))}
                  {pods.length === 0 && <tr><td colSpan={7} className="muted">No pods.</td></tr>}
                </tbody>
              </table>
            </div>
            {pod && <PodDetail key={pod.name} p={pod} claims={kube.claims} nodes={kube.nodes} canExec={canExec} onSelectHost={onSelectHost} onShell={setShellTarget} />}
            {!canExec && pods.length > 0 && <p className="muted">No shells in {w.namespace}: K3s and Longhorn run here — use kubectl exec. Logs still work.</p>}

      {volumes.length > 0 && (
        <>
          <h4>Volumes</h4>
            <div className="volume-cards">
              {volumes.map((v) => {
                const claim = v.pvc ? kube.claims.find((c) => `${c.namespace}/${c.name}` === v.pvc) : undefined
                const use = claim ? claimUse(claim) : null
                return (
                  <article key={v.name} className="volume-card">
                    <div><strong>{v.pvc ?? v.name}</strong><span className={volumeTone(v)}>{v.robustness === 'unknown' ? v.state : v.robustness}</span></div>
                    <dl>
                      <div><dt>Size</dt><dd>{gib(v.size_bytes)}</dd></div>
                      <div><dt>Replicas</dt><dd>{v.replicas.filter((r) => r.mode === 'RW').length}/{v.replicas_wanted} in sync</dd></div>
                      <div><dt>Attached</dt><dd>{v.node ?? 'detached'}</dd></div>
                      {use && <div><dt>Used</dt><dd>{use}</dd></div>}
                    </dl>
                    <div className="muted">{v.replicas.map((r) => `${r.node} ${r.mode ?? r.state}`).join(' · ')}</div>
                  </article>
                )
              })}
            </div>
        </>
      )}

      <h4>Warning events</h4>
      {          events.length === 0 ? <span className="muted">No warning events in the last hour.</span> : (
            <ul className="plain kube-events">
              {events.map((e, n) => (
                <li key={n}><span className="muted">{e.last ? `${age(e.last, now)} ago` : ''}</span> <strong className="warn">{e.reason}</strong>{e.count > 1 && <span className="muted"> ×{e.count}</span>} <span className="muted">{e.object}</span><div>{e.message}</div></li>
              ))}
            </ul>
        )}
      {shellTarget && (
        <Terminal key={`${shellTarget.pod}/${shellTarget.container}/${shellTarget.shell}`} url={execUrl({ namespace: w.namespace, pod: shellTarget.pod, container: shellTarget.container, shell: shellTarget.shell })} title={`${shellTarget.shell} in ${shellTarget.pod} (${shellTarget.container})`} onClose={() => setShellTarget(null)} />
      )}
    </aside>
  )
}
