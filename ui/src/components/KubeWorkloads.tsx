import { useEffect, useMemo, useState } from 'react'
import { kubeAction, kubeManifest } from '../api'
import type { KubeWorkload } from '../gen/KubeWorkload'
import { dock, logsSession } from '../dock'
import { useStore } from '../store'
import { age, bytesUsed, cpu, deletePrompt, netRate, usedTone, editDraft, filterKube, mem, podWhy, workloadKey, gib, podTone, replicaLabel, restartPrompt, volumeTone, workloadActions, workloadClaims, workloadTone, type KubeFilter } from '../kube'
import { ApplyDialog, type ApplyDraft } from './ApplyDialog'
import { EndpointLink, HealthBadge, Hosts, Usage } from './KubeBits'
import { useAction } from './useAction'

/** Pod tables can be long; the filters narrow them. */
const ROWS_SHOWN = 300

/** A workload's row: its replicas (editable, to scale it), Restart, Edit YAML and Delete, and Kubernetes' answer when it says no. */
function WorkloadRow({ w, hosts, onSelectHost, onSelect, onEdit }: { w: KubeWorkload; hosts: string[]; onSelectHost(id: string): void; onSelect(): void; onEdit(d: ApplyDraft): void }) {
  const pods = useStore((s) => s.kube.pods)
  const volumes = useStore((s) => s.kube.volumes)
  const { error, busy, run } = useAction()
  const [replicas, setReplicas] = useState(String(w.desired))
  // Follow changes made elsewhere (kubectl, another browser) unless a new count is being typed,
  // or was just asked for and Kubernetes has not reported it yet.
  const [editing, setEditing] = useState(false)
  useEffect(() => {
    if (!editing) setReplicas(String(w.desired))
    else if (String(w.desired) === replicas) setEditing(false)
  }, [w.desired, editing, replicas])
  const can = workloadActions(w)
  const workload = { namespace: w.namespace, kind: w.kind, name: w.name }
  const wanted = Number(replicas)
  const scalable = can.scale && replicas.trim() !== '' && Number.isInteger(wanted) && wanted >= 0 && wanted !== w.desired
  const scale = () => {
    if (!scalable) return
    void run(() =>
      kubeAction({ Scale: { workload, replicas: wanted } }).catch((e: unknown) => {
        setEditing(false)
        throw e
      }),
    )
  }

  return (
    <>
      <tr>
        <td className="muted">{w.namespace}</td>
        <td>
          <button className="link" onClick={onSelect} title="details">
            <strong>{w.name}</strong>
          </button>{' '}
          <span className="muted">{w.kind}</span>
        </td>
        <td className={`${workloadTone(w)} num`}>
          {w.ready}/
          {can.scale ? (
            <input
              type="number"
              min={0}
              value={replicas}
              disabled={busy}
              onChange={(e) => {
                setEditing(true)
                setReplicas(e.target.value)
              }}
              onKeyDown={(e) => e.key === 'Enter' && scale()}
              style={{ width: 52 }}
              aria-label={`replicas of ${w.name}`}
            />
          ) : (
            w.desired
          )}
          {scalable && (
            <>
              {' '}
              <button disabled={busy} onClick={scale}>
                Scale
              </button>{' '}
              <button
                className="link"
                disabled={busy}
                onClick={() => {
                  setEditing(false)
                  setReplicas(String(w.desired))
                }}
              >
                cancel
              </button>
            </>
          )}
        </td>
        <td>
          <HealthBadge health={w.health} />
        </td>
        <td className="num">
          <Usage used={w.cpu_milli} limit={w.cpu_limit_milli} format={cpu} />
        </td>
        <td className="num">
          <Usage used={w.memory_bytes} limit={w.memory_limit_bytes} format={mem} />
        </td>
        <td className="num muted">{netRate(w.rx_bps, w.tx_bps)}</td>
        <td>
          {w.endpoints.length > 0 ? <EndpointLink e={w.endpoints[0]} /> : <span className="muted">none</span>}
          {w.endpoints.length > 1 && (
            <button className="link muted" onClick={onSelect}>
              {' '}
              +{w.endpoints.length - 1}
            </button>
          )}
        </td>
        <td>
          <Hosts hosts={hosts} onSelectHost={onSelectHost} />
        </td>
        <td className="row-actions">
          {can.restart && (
            <button disabled={busy} onClick={() => window.confirm(restartPrompt(w)) && run(() => kubeAction({ Restart: { workload } }))}>
              Restart
            </button>
          )}{' '}
          {can.edit && (
            <button disabled={busy} onClick={() => run(async () => onEdit(editDraft(w, await kubeManifest(workload))))}>
              Edit YAML
            </button>
          )}{' '}
          {can.remove && (
            <button
              disabled={busy}
              onClick={() => window.confirm(deletePrompt(w, workloadClaims(w, pods, volumes))) && run(() => kubeAction({ Delete: { workload } }))}
            >
              Delete
            </button>
          )}
          {!can.remove && <span className="muted" title="K3s and Longhorn run here, installed and supervised by homelabd: change it with kubectl">system</span>}
        </td>
      </tr>
      {error && (
        <tr>
          <td colSpan={10}>
            <div className="bad refusal" role="alert">
              {error}
            </div>
          </td>
        </tr>
      )}
    </>
  )
}

/** What Kubernetes runs, as the server reads it from the K3s API: workloads, their pods and Longhorn's volumes. */
export function KubeWorkloads({ onSelectHost, onSelectWorkload }: { onSelectHost(id: string): void; onSelectWorkload(key: string): void }) {
  const kube = useStore((s) => s.kube)
  const now = useStore((s) => s.clock.now)
  const [filter, setFilter] = useState<KubeFilter>({ namespace: '', hideSystem: true, host: '', text: '' })
  const set = (patch: Partial<KubeFilter>) => setFilter((f) => ({ ...f, ...patch }))
  const rows = useMemo(() => filterKube(kube, filter), [kube, filter])
  const namespaces = useMemo(() => [...new Set(kube.pods.map((p) => p.namespace))].sort(), [kube.pods])
  const hosts = useMemo(() => [...new Set(kube.pods.flatMap((p) => (p.node ? [p.node] : [])))].sort(), [kube.pods])
  const [draft, setDraft] = useState<ApplyDraft | null>(null)

  return (
    <section className="workloads kube-workloads">
      <h3>Kubernetes</h3>
      <div className="fleet-bar">
        <label className="inline">
          Namespace{' '}
          <select value={filter.namespace} onChange={(e) => set({ namespace: e.target.value })} aria-label="namespace">
            <option value="">any</option>
            {namespaces.map((n) => (
              <option key={n}>{n}</option>
            ))}
          </select>
        </label>
        <label className="inline">
          <input type="checkbox" checked={filter.hideSystem} onChange={(e) => set({ hideSystem: e.target.checked })} /> hide K3s and Longhorn
        </label>
        <label className="inline">
          Host{' '}
          <select value={filter.host} onChange={(e) => set({ host: e.target.value })} aria-label="kube host">
            <option value="">any</option>
            {hosts.map((h) => (
              <option key={h}>{h}</option>
            ))}
          </select>
        </label>
        <input placeholder="search name" value={filter.text} onChange={(e) => set({ text: e.target.value })} aria-label="search kube name" size={16} />
        <button onClick={() => setDraft({ title: 'Apply YAML', yaml: '', namespace: filter.namespace, offerNew: true })}>Apply YAML…</button>
        <span className="muted">
          {kube.pods.length} pods in {kube.workloads.length} workloads, {kube.volumes.length} Longhorn volumes
          {kube.at > 0 && ` · read ${age(kube.at, now)} ago`}
        </span>
      </div>
      {kube.metrics_error && kube.at > 0 && <p className="muted">No CPU and memory usage: {kube.metrics_error}</p>}
      {kube.stats_error && kube.at > 0 && <p className="muted">{kube.stats_error}</p>}
      {kube.error && <p className={kube.at > 0 ? 'warn' : 'muted'}>{kube.at > 0 ? `Last read failed: ${kube.error}` : `Waiting for Kubernetes: ${kube.error}`}</p>}

      <h4>Workloads</h4>
      <table className="data workload-table">
        <thead>
          <tr>
            <th>Namespace</th>
            <th>Name</th>
            <th>Ready</th>
            <th>Health</th>
            <th>CPU</th>
            <th>Memory</th>
            <th>Network</th>
            <th>Endpoints</th>
            <th>Hosts</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {rows.workloads.map(({ workload: w, hosts }) => (
            <WorkloadRow key={workloadKey(w)} w={w} hosts={hosts} onSelectHost={onSelectHost} onSelect={() => onSelectWorkload(workloadKey(w))} onEdit={setDraft} />
          ))}
          {rows.workloads.length === 0 && (
            <tr>
              <td colSpan={10} className="muted">
                {kube.workloads.length === 0 ? 'None yet. Apply YAML adds some, or "Seed workloads" in the lab panel.' : 'Nothing matches the filters.'}
              </td>
            </tr>
          )}
        </tbody>
      </table>

      <h4>Pods</h4>
      <table className="data workload-table">
        <thead>
          <tr>
            <th>Namespace</th>
            <th>Name</th>
            <th>Status</th>
            <th>Ready</th>
            <th>Restarts</th>
            <th>CPU</th>
            <th>Memory</th>
            <th>Host</th>
            <th>Age</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {rows.pods.slice(0, ROWS_SHOWN).map((p) => (
            <tr key={`${p.namespace}/${p.name}`}>
              <td className="muted">{p.namespace}</td>
              <td title={p.owner ?? undefined}>
                <strong>{p.name}</strong>
              </td>
              <td className={podTone(p)} title={podWhy(p)}>
                {p.status}
              </td>
              <td>
                {p.ready}/{p.containers}
              </td>
              <td className={p.restarts > 0 ? 'warn' : 'muted'} title={podWhy(p)}>
                {p.restarts}
              </td>
              <td className="num">
                <Usage used={p.cpu_milli} limit={p.cpu_limit_milli} format={cpu} />
              </td>
              <td className="num">
                <Usage used={p.memory_bytes} limit={p.memory_limit_bytes} format={mem} />
              </td>
              <td>{p.node ? <Hosts hosts={[p.node]} onSelectHost={onSelectHost} /> : <span className="muted">unscheduled</span>}</td>
              <td className="muted">{age(p.created, now)}</td>
              <td className="row-actions">
                <button onClick={() => dock.open(logsSession(p))}>Logs</button>
              </td>
            </tr>
          ))}
          {rows.pods.length === 0 && (
            <tr>
              <td colSpan={10} className="muted">
                {kube.pods.length === 0 ? 'No pods yet.' : 'Nothing matches the filters.'}
              </td>
            </tr>
          )}
        </tbody>
      </table>
      {rows.pods.length > ROWS_SHOWN && (
        <p className="muted">
          Showing the first {ROWS_SHOWN} of {rows.pods.length}; narrow the filters to see the rest.
        </p>
      )}

      <h4>Longhorn volumes</h4>
      <table className="data workload-table">
        <thead>
          <tr>
            <th>Claim</th>
            <th>Used by</th>
            <th>Size</th>
            <th>Used</th>
            <th>Health</th>
            <th>Attached to</th>
            <th>Replicas</th>
          </tr>
        </thead>
        <tbody>
          {rows.volumes.map((v) => (
            <tr key={v.name}>
              <td title={v.name}>
                <strong>{v.pvc ?? v.name}</strong>
              </td>
              <td className="muted">{v.pods.join(', ') || 'nothing'}</td>
              <td>{gib(v.size_bytes)}</td>
              <td className={`num ${usedTone(v.used_bytes, v.size_bytes)}`}>{bytesUsed(v.used_bytes, v.size_bytes)}</td>
              <td className={volumeTone(v)}>
                {v.robustness === 'unknown' ? v.state : v.robustness}
              </td>
              <td>{v.node ? <Hosts hosts={[v.node]} onSelectHost={onSelectHost} /> : <span className="muted">detached</span>}</td>
              <td>
                {v.replicas.map((r, n) => {
                  const { text, tone } = replicaLabel(r)
                  return (
                    <span key={r.name} title={`${r.name} on ${r.disk}`}>
                      {n > 0 && ', '}
                      <button className="link" onClick={() => onSelectHost(r.node)}>
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
          ))}
          {rows.volumes.length === 0 && (
            <tr>
              <td colSpan={7} className="muted">
                {kube.volumes.length === 0 ? 'No volumes yet.' : 'Nothing matches the filters.'}
              </td>
            </tr>
          )}
        </tbody>
      </table>
      {draft && <ApplyDialog draft={draft} namespaces={namespaces} onClose={() => setDraft(null)} />}
    </section>
  )
}
