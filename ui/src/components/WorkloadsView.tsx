import { useMemo, useState } from 'react'
import { useStore } from '../store'
import {
  KIND_LABELS,
  buildWorkloads,
  filterWorkloads,
  healthTone,
  refOf,
  type WorkloadFilter,
  type WorkloadItem,
  type WorkloadKind,
  type WorkloadRef,
} from '../workloads'
import { CreateWorkload } from './CreateWorkload'
import { KubeWorkloads } from './KubeWorkloads'

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
  // Only a server with a lab reads Kubernetes; without one there is nothing to show.
  const hasKube = useStore((s) => s.kube.at > 0 || s.kube.error !== null)
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
      {hasKube && <KubeWorkloads onSelectHost={onSelectHost} />}
      {hasKube && (
        <>
          <h3>homelabd workloads</h3>
          <p className="muted">VMs, pods and volumes placed by homelabd's own scheduler. Real hosts do not run these yet; run workloads on Kubernetes.</p>
        </>
      )}
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
    </section>
  )
}
