import { useMemo, useState } from 'react'
import { useStore } from '../store'
import {
  KIND_LABELS,
  buildWorkloads,
  filterWorkloads,
  formatBytes,
  healthTone,
  refOf,
  type WorkloadFilter,
  type WorkloadItem,
  type WorkloadKind,
  type WorkloadRef,
} from '../workloads'
import { CreateWorkload } from './CreateWorkload'

/** Pod tables can have thousands of rows at scale; the filters narrow them. */
const ROWS_SHOWN = 300

const KINDS: (WorkloadKind | 'all')[] = ['all', 'vm', 'pod', 'volume']

function isSelected(item: WorkloadItem, selected: WorkloadRef | null): boolean {
  if (!selected) return false
  const r = refOf(item)
  return r.kind === selected.kind && r.id === selected.id
}

export function WorkloadsView({
  selected,
  onSelect,
  onSelectHost,
}: {
  selected: WorkloadRef | null
  onSelect(ref: WorkloadRef): void
  onSelectHost(id: string): void
}) {
  const deployments = useStore((s) => s.view.deployments)
  const pods = useStore((s) => s.view.pods)
  const vms = useStore((s) => s.view.vms)
  const vmStatus = useStore((s) => s.view.vmStatus)
  const volumes = useStore((s) => s.view.volumes)
  const volumeStatus = useStore((s) => s.view.volumeStatus)
  const [filter, setFilter] = useState<WorkloadFilter>({ kind: 'all', text: '', host: '', health: '' })
  const [creating, setCreating] = useState(false)
  const set = (patch: Partial<WorkloadFilter>) => setFilter((f) => ({ ...f, ...patch }))

  const items = useMemo(
    () => buildWorkloads({ deployments, pods, vms, vmStatus, volumes, volumeStatus }),
    [deployments, pods, vms, vmStatus, volumes, volumeStatus],
  )
  const hostOptions = useMemo(() => [...new Set(items.flatMap((i) => i.hosts))].sort(), [items])
  const healthOptions = useMemo(() => [...new Set(items.map((i) => i.health))].sort(), [items])
  const shown = useMemo(() => filterWorkloads(items, filter), [items, filter])
  const counts = { vm: Object.keys(vms).length, pod: Object.keys(pods).length, volume: Object.keys(volumes).length }

  return (
    <section className="workloads">
      <div className="fleet-bar">
        <label className="inline">
          Kind{' '}
          <select value={filter.kind} onChange={(e) => set({ kind: e.target.value as WorkloadFilter['kind'] })} aria-label="kind">
            {KINDS.map((k) => (
              <option key={k} value={k}>
                {k === 'all' ? 'all' : KIND_LABELS[k]}
              </option>
            ))}
          </select>
        </label>
        <label className="inline">
          Health{' '}
          <select value={filter.health} onChange={(e) => set({ health: e.target.value })} aria-label="health">
            <option value="">any</option>
            {healthOptions.map((h) => (
              <option key={h}>{h}</option>
            ))}
          </select>
        </label>
        <label className="inline">
          Host{' '}
          <select value={filter.host} onChange={(e) => set({ host: e.target.value })} aria-label="host">
            <option value="">any</option>
            {hostOptions.map((h) => (
              <option key={h}>{h}</option>
            ))}
          </select>
        </label>
        <input placeholder="search name" value={filter.text} onChange={(e) => set({ text: e.target.value })} aria-label="search name" size={16} />
        <span className="muted">
          {counts.vm} VMs, {counts.pod} pods in {Object.keys(deployments).length} deployments, {counts.volume} volumes
        </span>
        <div className="spacer" />
        <button className={creating ? 'active' : ''} onClick={() => setCreating(!creating)}>
          {creating ? 'Close form' : 'Create…'}
        </button>
      </div>
      {creating && <CreateWorkload />}
      <table className="data workload-table">
        <thead>
          <tr>
            <th>Kind</th>
            <th>Name</th>
            <th>Health</th>
            <th>Placement</th>
            <th>Detail</th>
          </tr>
        </thead>
        <tbody>
          {shown.slice(0, ROWS_SHOWN).map((i) => (
            <tr
              key={`${i.kind}:${i.id}`}
              className={`clickable${isSelected(i, selected) ? ' selected' : ''}`}
              onClick={() => onSelect(refOf(i))}
            >
              <td>{KIND_LABELS[i.kind]}</td>
              <td>
                <strong>{i.id}</strong>
              </td>
              <td className={healthTone(i.health)}>{i.health}</td>
              <td>
                {i.hosts.length === 0 ? (
                  <span className="muted">unplaced</span>
                ) : (
                  i.hosts.map((h, n) => (
                    <span key={h}>
                      {n > 0 && (i.kind === 'vm' ? ' → ' : ', ')}
                      <button
                        className="link"
                        onClick={(e) => {
                          e.stopPropagation()
                          onSelectHost(h)
                        }}
                      >
                        {h}
                      </button>
                    </span>
                  ))
                )}
              </td>
              <td className="muted">{i.detail}</td>
            </tr>
          ))}
          {shown.length === 0 && (
            <tr>
              <td colSpan={5} className="muted">
                {items.length === 0 ? 'No workloads yet. Create one, or seed some from the scenarios panel.' : 'Nothing matches the filters.'}
              </td>
            </tr>
          )}
        </tbody>
      </table>
      {shown.length > ROWS_SHOWN && (
        <p className="muted">
          Showing the first {ROWS_SHOWN} of {shown.length}; narrow the filters to see the rest.
        </p>
      )}
      <ObservedK8s onSelectHost={onSelectHost} />
      <ObservedLonghorn />
    </section>
  )
}

/** K8s node names are lowercased hostnames: the host id they belong to, if it is one of ours. */
function hostForNode(hosts: Record<string, unknown>, node: string): string | null {
  const id = Object.keys(hosts).find((h) => h.toLowerCase() === node.toLowerCase())
  return id ?? null
}

/** Deployments and pods as K3s reports them (read-only: our API does not manage them yet). */
function ObservedK8s({ onSelectHost }: { onSelectHost(id: string): void }) {
  const k8s = useStore((s) => s.view.k8s)
  const hosts = useStore((s) => s.view.hosts)
  if (!k8s) return null
  const nodeLink = (node: string) => {
    const id = hostForNode(hosts, node)
    return id ? (
      <button
        className="link"
        onClick={(e) => {
          e.stopPropagation()
          onSelectHost(id)
        }}
      >
        {id}
      </button>
    ) : (
      <span className="muted">{node || 'unplaced'}</span>
    )
  }
  return (
    <div className="section">
      <h4>Observed from K3s</h4>
      <p className="muted">What K3s runs, as the leader last observed it. Read-only.</p>
      <table className="data workload-table">
        <thead>
          <tr>
            <th>Kind</th>
            <th>Name</th>
            <th>Health</th>
            <th>Placement</th>
            <th>Detail</th>
          </tr>
        </thead>
        <tbody>
          {k8s.deployments.map((d) => (
            <tr key={`deployment:${d.namespace}/${d.name}`}>
              <td>Deployment</td>
              <td>
                <strong>
                  {d.namespace}/{d.name}
                </strong>
              </td>
              <td className={d.ready === d.total ? 'ok' : 'warn'}>
                {d.ready}/{d.total} ready
              </td>
              <td className="muted">—</td>
              <td className="muted">K3s</td>
            </tr>
          ))}
          {k8s.pods.slice(0, ROWS_SHOWN).map((p) => (
            <tr key={`pod:${p.namespace}/${p.name}`}>
              <td>Pod</td>
              <td>
                <strong>
                  {p.namespace}/{p.name}
                </strong>
              </td>
              <td className={healthTone(p.phase)}>{p.phase}</td>
              <td>{nodeLink(p.node)}</td>
              <td className="muted">K3s</td>
            </tr>
          ))}
          {k8s.deployments.length + k8s.pods.length === 0 && (
            <tr>
              <td colSpan={5} className="muted">
                K3s runs nothing yet.
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  )
}

/** Volumes and replica placement as Longhorn reports them (read-only). */
function ObservedLonghorn() {
  const k8s = useStore((s) => s.view.k8s)
  if (!k8s) return null
  const tone = (robustness: string) => healthTone(robustness.charAt(0).toUpperCase() + robustness.slice(1))
  return (
    <div className="section">
      <h4>Observed from Longhorn</h4>
      <p className="muted">What Longhorn stores, as the leader last observed it. Read-only.</p>
      <table className="data workload-table">
        <thead>
          <tr>
            <th>Volume</th>
            <th>Size</th>
            <th>Health</th>
            <th>Replicas</th>
          </tr>
        </thead>
        <tbody>
          {k8s.volumes.map((v) => (
            <tr key={v.name}>
              <td>
                <strong>{v.pvc ?? v.name}</strong>
                <div className="muted">{v.state}</div>
              </td>
              <td>{formatBytes(v.size_bytes)}</td>
              <td className={tone(v.robustness)}>{v.robustness}</td>
              <td>
                {v.replicas.length === 0 ? (
                  <span className="muted">none</span>
                ) : (
                  v.replicas.map((r, n) => (
                    <span key={`${r.node}:${n}`}>
                      {n > 0 && ', '}
                      {r.node} <span className={r.running ? 'ok' : 'bad'}>{r.running ? '●' : '○'}</span>
                    </span>
                  ))
                )}
              </td>
            </tr>
          ))}
          {k8s.volumes.length === 0 && (
            <tr>
              <td colSpan={4} className="muted">
                Longhorn stores no volumes yet.
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  )
}
