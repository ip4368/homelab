import { useEffect, useMemo, useState } from 'react'
import { kubeAction } from '../api'
import type { KubeWorkload } from '../gen/KubeWorkload'
import { useStore } from '../store'
import { age, deletePrompt, filterKube, gib, podTone, replicaLabel, restartPrompt, volumeTone, workloadActions, workloadClaims, workloadTone, type KubeFilter } from '../kube'
import { useAction } from './useAction'

/** Pod tables can be long; the filters narrow them. */
const ROWS_SHOWN = 300

function Hosts({ hosts, onSelectHost }: { hosts: string[]; onSelectHost(id: string): void }) {
  if (hosts.length === 0) return <span className="muted">none</span>
  return (
    <>
      {hosts.map((h, n) => (
        <span key={h}>
          {n > 0 && ', '}
          <button className="link" onClick={() => onSelectHost(h)}>
            {h}
          </button>
        </span>
      ))}
    </>
  )
}

/** A workload's row: its replicas (editable, to scale it), Restart and Delete, and Kubernetes' answer when it says no. */
function WorkloadRow({ w, hosts, onSelectHost }: { w: KubeWorkload; hosts: string[]; onSelectHost(id: string): void }) {
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
          <strong>{w.name}</strong>
        </td>
        <td>{w.kind}</td>
        <td className={workloadTone(w)}>
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
          <Hosts hosts={hosts} onSelectHost={onSelectHost} />
        </td>
        <td className="row-actions">
          {can.restart && (
            <button disabled={busy} onClick={() => window.confirm(restartPrompt(w)) && run(() => kubeAction({ Restart: { workload } }))}>
              Restart
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
          <td colSpan={6}>
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
export function KubeWorkloads({ onSelectHost }: { onSelectHost(id: string): void }) {
  const kube = useStore((s) => s.kube)
  const now = useStore((s) => s.clock.now)
  const [filter, setFilter] = useState<KubeFilter>({ namespace: '', hideSystem: true, host: '', text: '' })
  const set = (patch: Partial<KubeFilter>) => setFilter((f) => ({ ...f, ...patch }))
  const rows = useMemo(() => filterKube(kube, filter), [kube, filter])
  const namespaces = useMemo(() => [...new Set(kube.pods.map((p) => p.namespace))].sort(), [kube.pods])
  const hosts = useMemo(() => [...new Set(kube.pods.flatMap((p) => (p.node ? [p.node] : [])))].sort(), [kube.pods])

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
        <span className="muted">
          {kube.pods.length} pods in {kube.workloads.length} workloads, {kube.volumes.length} Longhorn volumes
          {kube.at > 0 && ` · read ${age(kube.at, now)} ago`}
        </span>
      </div>
      {kube.error && <p className={kube.at > 0 ? 'warn' : 'muted'}>{kube.at > 0 ? `Last read failed: ${kube.error}` : `Waiting for Kubernetes: ${kube.error}`}</p>}

      <h4>Workloads</h4>
      <table className="data workload-table">
        <thead>
          <tr>
            <th>Namespace</th>
            <th>Name</th>
            <th>Kind</th>
            <th>Ready</th>
            <th>Hosts</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {rows.workloads.map(({ workload: w, hosts }) => (
            <WorkloadRow key={`${w.namespace}/${w.kind}/${w.name}`} w={w} hosts={hosts} onSelectHost={onSelectHost} />
          ))}
          {rows.workloads.length === 0 && (
            <tr>
              <td colSpan={6} className="muted">
                {kube.workloads.length === 0 ? 'None yet. "Seed workloads" in the lab panel adds a few.' : 'Nothing matches the filters.'}
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
            <th>Host</th>
            <th>Age</th>
          </tr>
        </thead>
        <tbody>
          {rows.pods.slice(0, ROWS_SHOWN).map((p) => (
            <tr key={`${p.namespace}/${p.name}`}>
              <td className="muted">{p.namespace}</td>
              <td title={p.owner ?? undefined}>
                <strong>{p.name}</strong>
              </td>
              <td className={podTone(p)}>{p.status}</td>
              <td>
                {p.ready}/{p.containers}
              </td>
              <td className={p.restarts > 0 ? 'warn' : 'muted'}>{p.restarts}</td>
              <td>{p.node ? <Hosts hosts={[p.node]} onSelectHost={onSelectHost} /> : <span className="muted">unscheduled</span>}</td>
              <td className="muted">{age(p.created, now)}</td>
            </tr>
          ))}
          {rows.pods.length === 0 && (
            <tr>
              <td colSpan={7} className="muted">
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
              <td colSpan={6} className="muted">
                {kube.volumes.length === 0 ? 'No volumes yet.' : 'Nothing matches the filters.'}
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </section>
  )
}
