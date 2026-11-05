import { useEffect, useMemo, useState } from 'react'
import type { KubePod } from '../gen/KubePod'
import { age, bytesUsed, cpu, filterKube, gib, healthTone, HEALTH_LABELS, mem, podTone, podWhy, replicaLabel, usedTone, volumeTone, workloadKey, type KubeFilter } from '../kube'
import { nav } from '../nav'
import { useStore } from '../store'
import { ApplyDialog, type ApplyDraft } from './ApplyDialog'
import { Icon } from './Icons'
import { EndpointLink, Hosts, NetRate, Usage } from './KubeBits'
import { PodButtons } from './PodButtons'
import { ScaleControl, WorkloadRowActions } from './WorkloadActions'

/** Pod tables can be long; the filters narrow them. */
const ROWS_SHOWN = 300

export const LISTS = ['workloads', 'pods', 'volumes'] as const
/** The filters, kept while another page shows. */
let kept: { filter: KubeFilter; problems: boolean } = { filter: { namespace: '', hideSystem: true, host: '', text: '' }, problems: false }
type List = (typeof LISTS)[number]

/** The workload a pod belongs to, as the detail pane opens it. */
function ownerKey(p: KubePod): string | null {
  return p.owner ? `${p.namespace}/${p.owner}` : null
}

/**
 * What Kubernetes runs, as the server reads it from the K3s API: one list at
 * a time (workloads, pods or Longhorn volumes) under one set of filters.
 * Clicking a row opens its workload in the detail pane.
 */
export function WorkloadsPage({ list, selected }: { list: string | null; selected: string | null }) {
  const kube = useStore((s) => s.kube)
  const now = useStore((s) => s.clock.now)
  const shown: List = LISTS.find((l) => l === list) ?? 'workloads'
  const [filter, setFilter] = useState<KubeFilter>(kept.filter)
  const [problems, setProblems] = useState(kept.problems)
  useEffect(() => {
    kept = { filter, problems }
  }, [filter, problems])
  const set = (patch: Partial<KubeFilter>) => setFilter((f) => ({ ...f, ...patch }))
  const filtered = useMemo(() => filterKube(kube, filter), [kube, filter])
  const rows = useMemo(
    () =>
      problems
        ? {
            workloads: filtered.workloads.filter(({ workload: w }) => w.health.state !== 'Healthy' && w.health.state !== 'Idle'),
            pods: filtered.pods.filter((p) => podTone(p) === 'bad' || podTone(p) === 'warn' || p.restarts > 0),
            volumes: filtered.volumes.filter((v) => volumeTone(v) === 'bad' || volumeTone(v) === 'warn'),
          }
        : filtered,
    [filtered, problems],
  )
  const namespaces = useMemo(() => [...new Set(kube.pods.map((p) => p.namespace))].sort(), [kube.pods])
  const hosts = useMemo(() => [...new Set(kube.pods.flatMap((p) => (p.node ? [p.node] : [])))].sort(), [kube.pods])
  const [draft, setDraft] = useState<ApplyDraft | null>(null)
  const counts = { workloads: rows.workloads.length, pods: rows.pods.length, volumes: rows.volumes.length }
  const ownerOfPod = (name: string) => kube.pods.find((p) => p.name === name)
  const onHost = (h: string) => nav.host(h)

  return (
    <section className="workloads-page">
      <div className="toolbar">
        <div className="seg" role="tablist" aria-label="list">
          {LISTS.map((l) => (
            <button key={l} role="tab" aria-selected={l === shown} className={l === shown ? 'active' : ''} onClick={() => nav.list(l === 'workloads' ? null : l)}>
              {l === 'workloads' ? 'Workloads' : l === 'pods' ? 'Pods' : 'Volumes'}
              <span className="count">{counts[l]}</span>
            </button>
          ))}
        </div>
        <input type="search" placeholder="Filter by name…" value={filter.text} onChange={(e) => set({ text: e.target.value })} aria-label="search kube name" />
        <select value={filter.namespace} onChange={(e) => set({ namespace: e.target.value })} aria-label="namespace">
          <option value="">all namespaces</option>
          {namespaces.map((n) => (
            <option key={n}>{n}</option>
          ))}
        </select>
        <select value={filter.host} onChange={(e) => set({ host: e.target.value })} aria-label="kube host">
          <option value="">all hosts</option>
          {hosts.map((h) => (
            <option key={h}>{h}</option>
          ))}
        </select>
        <label className={`chip-toggle${problems ? ' on' : ''}`} title="Workloads not healthy, pods not running well or restarted, volumes not healthy">
          <input type="checkbox" checked={problems} onChange={(e) => setProblems(e.target.checked)} /> Problems only
        </label>
        <label className={`chip-toggle${!filter.hideSystem ? ' on' : ''}`} title="K3s, Longhorn and KubeVirt's own namespaces">
          <input type="checkbox" checked={!filter.hideSystem} onChange={(e) => set({ hideSystem: !e.target.checked })} /> System
        </label>
        <div className="spacer" />
        <button className="primary" onClick={() => setDraft({ title: 'Apply YAML', yaml: '', namespace: filter.namespace, offerNew: true })}>
          <Icon name="plus" size={14} /> New / Apply YAML
        </button>
      </div>
      <div className="status-line muted">
        {kube.pods.length} pods in {kube.workloads.length} workloads, {kube.volumes.length} Longhorn volumes
        {kube.at > 0 && ` · read ${age(kube.at, now)} ago`}
        {kube.kubevirt && (
          <span className={kube.kubevirt === 'Deployed' ? undefined : 'warn'} title="VMs run on the hosts with the vm role">
            {' '}
            · KubeVirt {kube.kubevirt.toLowerCase()}
          </span>
        )}
        {kube.metrics_error && kube.at > 0 && <span> · no CPU and memory usage: {kube.metrics_error}</span>}
        {kube.stats_error && kube.at > 0 && <span> · {kube.stats_error}</span>}
      </div>
      {kube.error && <p className={kube.at > 0 ? 'warn' : 'muted'}>{kube.at > 0 ? `Last read failed: ${kube.error}` : `Waiting for Kubernetes: ${kube.error}`}</p>}

      {shown === 'workloads' && (
        <div className="table-wrap card flush">
          <table className="grid-table workload-table">
            <thead>
              <tr>
                <th>Workload</th>
                <th>Ready</th>
                <th>Health</th>
                <th className="num hide-compact">CPU</th>
                <th className="num hide-compact">Memory</th>
                <th className="num hide-mid">Network</th>
                <th className="hide-narrow">Endpoint</th>
                <th className="hide-narrow">Hosts</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {rows.workloads.map(({ workload: w, hosts }) => {
                const key = workloadKey(w)
                return (
                  <tr key={key} className={`clickable${key === selected ? ' selected' : ''}`} onClick={() => nav.workload(key)}>
                    <td>
                      <span className="cell-title">{w.name}</span>
                      <div className="cell-sub">
                        {w.kind} · {w.namespace}
                      </div>
                    </td>
                    <td>
                      <ScaleControl w={w} />
                    </td>
                    <td title={w.health.reason ?? undefined}>
                      <span className={`pill ${healthTone(w.health.state)}`}>{HEALTH_LABELS[w.health.state]}</span>
                      {w.health.reason && <div className="cell-sub clip">{w.health.reason}</div>}
                    </td>
                    <td className="num hide-compact">
                      <Usage used={w.cpu_milli} limit={w.cpu_limit_milli} format={cpu} />
                    </td>
                    <td className="num hide-compact">
                      <Usage used={w.memory_bytes} limit={w.memory_limit_bytes} format={mem} />
                    </td>
                    <td className="num muted hide-mid">
                      <NetRate rx={w.rx_bps} tx={w.tx_bps} />
                    </td>
                    <td className="hide-narrow" onClick={(e) => e.stopPropagation()}>
                      {w.endpoints.length > 0 ? <EndpointLink e={w.endpoints[0]} /> : <span className="muted">–</span>}
                      {w.endpoints.length > 1 && <span className="muted"> +{w.endpoints.length - 1}</span>}
                    </td>
                    <td className="hide-narrow" onClick={(e) => e.stopPropagation()}>
                      <Hosts hosts={hosts} onSelectHost={onHost} />
                    </td>
                    <td className="row-actions">
                      <WorkloadRowActions w={w} />
                    </td>
                  </tr>
                )
              })}
              {rows.workloads.length === 0 && (
                <tr>
                  <td colSpan={9} className="muted">
                    {kube.workloads.length === 0 ? 'None yet. New / Apply YAML adds some, or "Seed workloads" on the Lab page.' : 'Nothing matches the filters.'}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}

      {shown === 'pods' && (
        <>
          <div className="table-wrap card flush">
            <table className="grid-table">
              <thead>
                <tr>
                  <th>Pod</th>
                  <th>Status</th>
                  <th className="num hide-compact">CPU</th>
                  <th className="num hide-compact">Memory</th>
                  <th className="hide-compact">Host</th>
                  <th className="num hide-narrow">Age</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {rows.pods.slice(0, ROWS_SHOWN).map((p) => {
                  const owner = ownerKey(p)
                  return (
                    <tr key={`${p.namespace}/${p.name}`} className={`${owner ? 'clickable' : ''}${owner === selected ? ' selected' : ''}`} onClick={() => owner && nav.workload(owner, 'pods')}>
                      <td>
                        <span className="cell-title">{p.name}</span>
                        <div className="cell-sub">
                          {p.namespace}
                          {p.owner && ` · ${p.owner}`}
                        </div>
                      </td>
                      <td title={podWhy(p)}>
                        <span className={`pill ${podTone(p)}`}>{p.status}</span>{' '}
                        <span className="muted">
                          {p.ready}/{p.containers}
                        </span>
                        {p.restarts > 0 && (
                          <span className="warn">
                            {' '}
                            · {p.restarts} restart{p.restarts === 1 ? '' : 's'}
                          </span>
                        )}
                        {podWhy(p) && <div className="cell-sub clip">{podWhy(p)}</div>}
                      </td>
                      <td className="num hide-compact">
                        <Usage used={p.cpu_milli} limit={p.cpu_limit_milli} format={cpu} />
                      </td>
                      <td className="num hide-compact">
                        <Usage used={p.memory_bytes} limit={p.memory_limit_bytes} format={mem} />
                      </td>
                      <td className="hide-compact" onClick={(e) => e.stopPropagation()}>{p.node ? <Hosts hosts={[p.node]} onSelectHost={onHost} /> : <span className="muted">unscheduled</span>}</td>
                      <td className="num muted hide-narrow">{age(p.created, now)}</td>
                      <td className="row-actions">
                        <PodButtons pod={p} />
                      </td>
                    </tr>
                  )
                })}
                {rows.pods.length === 0 && (
                  <tr>
                    <td colSpan={7} className="muted">
                      {kube.pods.length === 0 ? 'No pods yet.' : 'Nothing matches the filters.'}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
          {rows.pods.length > ROWS_SHOWN && (
            <p className="muted">
              Showing the first {ROWS_SHOWN} of {rows.pods.length}; narrow the filters to see the rest.
            </p>
          )}
        </>
      )}

      {shown === 'volumes' && (
        <div className="table-wrap card flush">
          <table className="grid-table">
            <thead>
              <tr>
                <th>Claim</th>
                <th>Health</th>
                <th className="num hide-compact">Size</th>
                <th className="num">Used</th>
                <th className="hide-compact">Used by</th>
                <th className="hide-narrow">Attached to</th>
                <th>Replicas</th>
              </tr>
            </thead>
            <tbody>
              {rows.volumes.map((v) => {
                const pod = v.pods.map(ownerOfPod).find((p) => p && ownerKey(p))
                const owner = pod ? ownerKey(pod) : null
                return (
                  <tr key={v.name} className={owner ? 'clickable' : undefined} onClick={() => owner && nav.workload(owner)} title={v.name}>
                    <td>
                      <span className="cell-title">{v.pvc ?? v.name}</span>
                      {v.pvc && <div className="cell-sub">{v.name}</div>}
                    </td>
                    <td>
                      <span className={`pill ${volumeTone(v)}`}>{v.robustness === 'unknown' ? v.state : v.robustness}</span>
                    </td>
                    <td className="num hide-compact">{gib(v.size_bytes)}</td>
                    <td className={`num ${usedTone(v.used_bytes, v.size_bytes)}`}>{bytesUsed(v.used_bytes, v.size_bytes)}</td>
                    <td className="muted hide-compact">{v.pods.join(', ') || 'nothing'}</td>
                    <td className="hide-narrow" onClick={(e) => e.stopPropagation()}>
                      {v.node ? <Hosts hosts={[v.node]} onSelectHost={onHost} /> : <span className="muted">detached</span>}
                    </td>
                    <td onClick={(e) => e.stopPropagation()}>
                      {v.replicas.map((r, n) => {
                        const { text, tone } = replicaLabel(r)
                        return (
                          <span key={r.name} title={`${r.name} on ${r.disk}`}>
                            {n > 0 && ', '}
                            <button className="link" onClick={() => onHost(r.node)}>
                              {r.node}
                            </button>{' '}
                            <span className={tone}>{text}</span>
                          </span>
                        )
                      })}
                      <span className="muted">
                        {' '}
                        ({v.replicas.filter((r) => r.mode === 'RW').length}/{v.replicas_wanted} in sync)
                      </span>
                    </td>
                  </tr>
                )
              })}
              {rows.volumes.length === 0 && (
                <tr>
                  <td colSpan={7} className="muted">
                    {kube.volumes.length === 0 ? 'No volumes yet.' : 'Nothing matches the filters.'}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}
      {draft && <ApplyDialog draft={draft} namespaces={namespaces} onClose={() => setDraft(null)} />}
    </section>
  )
}
