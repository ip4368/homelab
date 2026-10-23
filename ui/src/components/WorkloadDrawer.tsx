import { useState } from 'react'
import { op } from '../api'
import type { ApiOp } from '../gen/ApiOp'
import type { ReplicaRef } from '../gen/ReplicaRef'
import { useStore } from '../store'
import { healthTone, migrationTargets, podPhaseName, replicaStateLabel, vmPhaseName, vmPhaseNote, type WorkloadRef } from '../workloads'
import { useAction } from './useAction'

const AUTO = ''

function HostLink({ id, onSelectHost }: { id: string; onSelectHost(id: string): void }) {
  return (
    <button className="link" onClick={() => onSelectHost(id)}>
      {id}
    </button>
  )
}

/** One row per replica: where it is, its state, and how far a rebuild has got. */
function ReplicaMap({ replicas, onSelectHost }: { replicas: ReplicaRef[]; onSelectHost(id: string): void }) {
  if (replicas.length === 0) return <p className="muted">No replicas placed yet.</p>
  return (
    <table className="data replica-map">
      <thead>
        <tr>
          <th>Host</th>
          <th>Disk</th>
          <th>State</th>
          <th>Progress</th>
        </tr>
      </thead>
      <tbody>
        {replicas.map((r) => {
          const pct = typeof r.state === 'object' ? r.state.Rebuilding.pct : r.state === 'Healthy' ? 100 : 0
          const label = typeof r.state === 'string' ? r.state : 'Rebuilding'
          return (
            <tr key={`${r.host}/${r.disk}`}>
              <td>
                <HostLink id={r.host} onSelectHost={onSelectHost} />
              </td>
              <td>{r.disk}</td>
              <td className={healthTone(label)} title={replicaStateLabel(r.state)}>
                {replicaStateLabel(r.state)}
              </td>
              <td style={{ width: 90 }}>
                <div className="bar" title={`${Math.round(pct)}%`}>
                  <div className={`bar-fill ${r.state === 'Stale' ? 'bad' : ''}`} style={{ width: `${Math.max(0, Math.min(100, pct))}%` }} />
                </div>
              </td>
            </tr>
          )
        })}
      </tbody>
    </table>
  )
}

/** Delete with a second click to confirm; the server's refusal (a volume in use, say) shows beside it. */
function DeleteButton({ label, o, disabled, onDeleted }: { label: string; o: ApiOp; disabled: boolean; onDeleted(): void }) {
  const [confirming, setConfirming] = useState(false)
  const { error, busy, run } = useAction()
  return (
    <div className="section">
      <div className="actions">
        {confirming ? (
          <>
            <button
              className="danger"
              disabled={disabled || busy}
              onClick={() =>
                run(async () => {
                  await op(o)
                  onDeleted()
                })
              }
            >
              Confirm delete
            </button>
            <button onClick={() => setConfirming(false)}>Cancel</button>
          </>
        ) : (
          <button disabled={disabled} onClick={() => setConfirming(true)}>
            {label}
          </button>
        )}
        {error && (
          <span className="bad" role="alert">
            {error}
          </span>
        )}
      </div>
    </div>
  )
}

function MigrateForm({ vm, from, running, paused }: { vm: string; from: string | null; running: boolean; paused: boolean }) {
  const hosts = useStore((s) => s.view.hosts)
  const [target, setTarget] = useState(AUTO)
  const { error, busy, run } = useAction()
  const targets = migrationTargets(hosts, from)
  // A target that stopped being eligible falls back to automatic placement.
  const chosen = targets.includes(target) ? target : AUTO
  return (
    <div className="section">
      <h4>Migrate</h4>
      <div className="actions">
        <label className="inline">
          to{' '}
          <select value={chosen} onChange={(e) => setTarget(e.target.value)} aria-label="migration target">
            <option value={AUTO}>auto (scheduler picks)</option>
            {targets.map((h) => (
              <option key={h}>{h}</option>
            ))}
          </select>
        </label>
        <button disabled={paused || busy || !running} onClick={() => run(() => op({ MigrateVm: { vm, to: chosen === AUTO ? null : chosen } }))}>
          Migrate
        </button>
      </div>
      {!running && <div className="muted">Only a running VM can be migrated.</div>}
      {error && (
        <div className="bad" role="alert">
          {error}
        </div>
      )}
    </div>
  )
}

function VmDetail({ id, onSelectHost, onSelect, onClose }: DetailProps) {
  const vm = useStore((s) => s.view.vms[id])
  const status = useStore((s) => s.view.vmStatus[id])
  const volume = useStore((s) => (vm ? s.view.volumeStatus[vm.volume] : undefined))
  const paused = useStore((s) => s.sim.paused)
  if (!vm) return null
  const phase = status ? vmPhaseName(status.phase) : 'Pending'
  const note = status ? vmPhaseNote(status.phase) : ''
  return (
    <>
      <Head title={vm.id} badge={phase} onClose={onClose} />
      <dl className="inventory">
        <dt>State</dt>
        <dd className={healthTone(phase)}>
          {phase}
          {note && <span className="muted"> {note}</span>}
        </dd>
        <dt>Host</dt>
        <dd>{status?.host ? <HostLink id={status.host} onSelectHost={onSelectHost} /> : <span className="muted">none</span>}</dd>
        <dt>Size</dt>
        <dd>
          {vm.vcpu} vCPU, {vm.ram_mb} MB RAM, {vm.disk_gb} GB disk
        </dd>
        <dt>Networks</dt>
        <dd>{vm.networks.join(', ') || <span className="muted">none</span>}</dd>
        <dt>Volume</dt>
        <dd>
          <button className="link" onClick={() => onSelect({ kind: 'volume', id: vm.volume })}>
            {vm.volume}
          </button>
        </dd>
      </dl>
      <MigrateForm vm={vm.id} from={status?.host ?? null} running={status?.phase === 'Running'} paused={paused} />
      <div className="section">
        <h4>Disk replicas ({vm.volume})</h4>
        {volume ? <ReplicaMap replicas={volume.replicas} onSelectHost={onSelectHost} /> : <p className="muted">Volume not found.</p>}
      </div>
      <DeleteButton label="Delete VM" o={{ DeleteVm: { id: vm.id } }} disabled={paused} onDeleted={onClose} />
    </>
  )
}

function VolumeDetail({ id, onSelectHost, onSelect, onClose }: DetailProps) {
  const volume = useStore((s) => s.view.volumes[id])
  const status = useStore((s) => s.view.volumeStatus[id])
  const vms = useStore((s) => s.view.vms)
  const paused = useStore((s) => s.sim.paused)
  if (!volume) return null
  const health = status?.health ?? 'Faulted'
  const users = Object.values(vms)
    .filter((v) => v.volume === id)
    .map((v) => v.id)
    .sort()
  return (
    <>
      <Head title={volume.id} badge={health} onClose={onClose} />
      <dl className="inventory">
        <dt>Health</dt>
        <dd className={healthTone(health)}>{health}</dd>
        <dt>Size</dt>
        <dd>{volume.size_gb} GB</dd>
        <dt>Replicas</dt>
        <dd>
          {status?.replicas.filter((r) => r.state === 'Healthy').length ?? 0} healthy of {volume.replicas} wanted
        </dd>
        <dt>Used by</dt>
        <dd>
          {users.length === 0 ? (
            <span className="muted">no VM</span>
          ) : (
            users.map((u, n) => (
              <span key={u}>
                {n > 0 && ', '}
                <button className="link" onClick={() => onSelect({ kind: 'vm', id: u })}>
                  {u}
                </button>
              </span>
            ))
          )}
        </dd>
      </dl>
      <div className="section">
        <h4>Replicas</h4>
        <ReplicaMap replicas={status?.replicas ?? []} onSelectHost={onSelectHost} />
      </div>
      <DeleteButton label="Delete volume" o={{ DeleteVolume: { id } }} disabled={paused} onDeleted={onClose} />
    </>
  )
}

function DeploymentDetail({ id, onSelectHost, onClose }: DetailProps) {
  const deployment = useStore((s) => s.view.deployments[id])
  const allPods = useStore((s) => s.view.pods)
  const paused = useStore((s) => s.sim.paused)
  if (!deployment) return null
  const pods = Object.values(allPods)
    .filter((p) => p.deployment === id)
    .sort((a, b) => a.idx - b.idx)
  const running = pods.filter((p) => p.phase === 'Running').length
  const needs = ['Compute', ...deployment.selector.roles.filter((r) => r !== 'Compute')]
  return (
    <>
      <Head title={deployment.id} badge={`${running}/${deployment.replicas} running`} onClose={onClose} />
      <dl className="inventory">
        <dt>Image</dt>
        <dd>{deployment.image}</dd>
        <dt>Pod size</dt>
        <dd>
          {deployment.cpu_m} m CPU, {deployment.ram_mb} MB RAM
        </dd>
        <dt>Needs</dt>
        <dd>
          {needs.join(', ')}
          {deployment.selector.capability_prefix && <span className="muted"> · capability {deployment.selector.capability_prefix}*</span>}
        </dd>
      </dl>
      <div className="section">
        <h4>Pods</h4>
        <table className="data">
          <thead>
            <tr>
              <th>Pod</th>
              <th>Phase</th>
              <th>Host</th>
            </tr>
          </thead>
          <tbody>
            {pods.slice(0, 200).map((p) => {
              const phase = podPhaseName(p.phase)
              return (
                <tr key={p.idx}>
                  <td>
                    {p.deployment}/{p.idx}
                  </td>
                  <td className={healthTone(phase)} title={typeof p.phase === 'object' ? p.phase.Pending : undefined}>
                    {phase}
                    {typeof p.phase === 'object' && <span className="muted"> {p.phase.Pending}</span>}
                  </td>
                  <td>{p.host ? <HostLink id={p.host} onSelectHost={onSelectHost} /> : <span className="muted">unplaced</span>}</td>
                </tr>
              )
            })}
            {pods.length === 0 && (
              <tr>
                <td colSpan={3} className="muted">
                  No pods.
                </td>
              </tr>
            )}
          </tbody>
        </table>
        {pods.length > 200 && <p className="muted">Showing the first 200 of {pods.length} pods.</p>}
      </div>
      <DeleteButton label="Delete deployment" o={{ DeleteDeployment: { id } }} disabled={paused} onDeleted={onClose} />
    </>
  )
}

interface DetailProps {
  id: string
  onSelectHost(id: string): void
  onSelect(ref: WorkloadRef): void
  onClose(): void
}

function Head({ title, badge, onClose }: { title: string; badge: string; onClose(): void }) {
  return (
    <div className="drawer-head">
      <h3>{title}</h3>
      <span className="lifecycle">{badge}</span>
      <div className="spacer" />
      <button onClick={onClose} aria-label="close">
        ×
      </button>
    </div>
  )
}

/** The drawer for a VM, volume or deployment (a pod opens its deployment). */
export function WorkloadDrawer({ workload, ...rest }: { workload: WorkloadRef } & Omit<DetailProps, 'id'>) {
  const paused = useStore((s) => s.sim.paused)
  return (
    <aside className="drawer workload-drawer" aria-label={`${workload.kind} ${workload.id}`}>
      {paused && <p className="muted">Paused: actions are disabled.</p>}
      {workload.kind === 'vm' && <VmDetail id={workload.id} {...rest} />}
      {workload.kind === 'volume' && <VolumeDetail id={workload.id} {...rest} />}
      {workload.kind === 'deployment' && <DeploymentDetail id={workload.id} {...rest} />}
    </aside>
  )
}
