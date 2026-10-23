import { useMemo, useState } from 'react'
import { op } from '../api'
import type { HostObj } from '../gen/HostObj'
import type { Role } from '../gen/Role'
import { useStore } from '../store'
import { hostWorkloads, replicaStateLabel, type WorkloadRef } from '../workloads'
import { NetworkEditor } from './NetworkEditor'
import { RolePicker } from './RolePicker'
import { useAction } from './useAction'

function sameRoles(a: Role[], b: Role[]): boolean {
  return a.length === b.length && a.every((r) => b.includes(r))
}

function Inventory({ host }: { host: HostObj }) {
  const { profile, inventory } = host
  return (
    <dl className="inventory">
      <dt>Profile</dt>
      <dd>
        {profile.id} ({profile.arch}, {profile.cpu_threads} threads, {profile.ram_gb} GB)
      </dd>
      <dt>Location</dt>
      <dd>{host.location}</dd>
      <dt>NICs</dt>
      <dd>
        {inventory.nics.map((n) => (
          <div key={n.name} className={n.link_up ? '' : 'bad'}>
            {n.name} {n.speed_mbps} Mb/s {n.link_up ? 'up' : 'down'}
          </div>
        ))}
      </dd>
      <dt>Disks</dt>
      <dd>
        {inventory.disks.map((d) => (
          <div key={d.name} className={d.failed ? 'bad' : ''}>
            {d.name} {d.kind} {d.size_gb} GB{d.failed ? ' failed' : ''}
          </div>
        ))}
      </dd>
      {profile.capabilities.length > 0 && (
        <>
          <dt>Capabilities</dt>
          <dd>{profile.capabilities.join(', ')}</dd>
        </>
      )}
      {Object.keys(host.labels).length > 0 && (
        <>
          <dt>Labels</dt>
          <dd>
            {Object.entries(host.labels)
              .map(([k, v]) => `${k}=${v}`)
              .join(', ')}
          </dd>
        </>
      )}
    </dl>
  )
}

/** Roles editor: Approve while the host is Pending, otherwise Apply (SetRoles). */
function RolesEditor({ host, disabled }: { host: HostObj; disabled: boolean }) {
  const pending = host.lifecycle === 'Pending'
  const [roles, setRoles] = useState<Role[]>(pending && host.roles.length === 0 ? ['Compute'] : host.roles)
  const { error, busy, run } = useAction()
  const submit = () =>
    run(() => op(pending ? { ApproveHost: { id: host.id, roles } } : { SetRoles: { id: host.id, roles } }))
  return (
    <div className="section">
      <h4>Roles</h4>
      <RolePicker profile={host.profile} value={roles} onChange={setRoles} />
      <div className="actions">
        <button onClick={submit} disabled={disabled || busy || (!pending && sameRoles(roles, host.roles))}>
          {pending ? 'Approve' : 'Apply roles'}
        </button>
        {error && <span className="bad">{error}</span>}
      </div>
    </div>
  )
}

const MAX_LISTED = 12

/** What the view places on the host. While it drains, this is what is left to move off. */
function HostWorkloads({ host, onSelectWorkload }: { host: HostObj; onSelectWorkload(ref: WorkloadRef): void }) {
  const pods = useStore((s) => s.view.pods)
  const vmStatus = useStore((s) => s.view.vmStatus)
  const volumeStatus = useStore((s) => s.view.volumeStatus)
  const { vms, pods: podIds, replicas } = useMemo(
    () => hostWorkloads({ pods, vmStatus, volumeStatus }, host.id),
    [pods, vmStatus, volumeStatus, host.id],
  )
  const draining = host.lifecycle === 'Draining'
  const none = vms.length + podIds.length + replicas.length === 0
  const link = (ref: WorkloadRef, label = ref.id) => (
    <button key={`${ref.kind}:${label}`} className="link" onClick={() => onSelectWorkload(ref)}>
      {label}
    </button>
  )
  const list = <T,>(items: T[], render: (t: T) => React.ReactNode) => (
    <>
      {items.slice(0, MAX_LISTED).map((t, n) => (
        <span key={n}>
          {n > 0 && ', '}
          {render(t)}
        </span>
      ))}
      {items.length > MAX_LISTED && <span className="muted"> and {items.length - MAX_LISTED} more</span>}
    </>
  )
  return (
    <div className="section">
      <h4>Workloads</h4>
      {draining && (
        <p className="drain-progress" role="status">
          Draining: {vms.length} VM{vms.length === 1 ? '' : 's'}, {podIds.length} pod{podIds.length === 1 ? '' : 's'}, {replicas.length} volume replica
          {replicas.length === 1 ? '' : 's'} left to move off.
        </p>
      )}
      {host.lifecycle === 'Drained' && none && <p className="ok">Drained: nothing runs here.</p>}
      {none && !draining && host.lifecycle !== 'Drained' && <p className="muted">Nothing placed on this host.</p>}
      <dl className="inventory">
        {vms.length > 0 && (
          <>
            <dt>VMs</dt>
            <dd>{list(vms, (v) => link({ kind: 'vm', id: v }))}</dd>
          </>
        )}
        {podIds.length > 0 && (
          <>
            <dt>Pods</dt>
            <dd>{list(podIds, (p) => link({ kind: 'deployment', id: p.slice(0, p.lastIndexOf('/')) }, p))}</dd>
          </>
        )}
        {replicas.length > 0 && (
          <>
            <dt>Replicas</dt>
            <dd>
              {list(replicas, (r) => (
                <>
                  {link({ kind: 'volume', id: r.volume })} <span className="muted">{r.disk} {replicaStateLabel(r.state)}</span>
                </>
              ))}
            </dd>
          </>
        )}
      </dl>
    </div>
  )
}

export function HostDrawer({ id, onClose, onSelectWorkload }: { id: string; onClose(): void; onSelectWorkload(ref: WorkloadRef): void }) {
  const host = useStore((s) => s.view.hosts[id])
  const paused = useStore((s) => s.sim.paused)
  const { error, busy, run } = useAction()
  if (!host) return null

  const lifecycleButton = (label: string, show: boolean, o: Parameters<typeof op>[0]) =>
    show && (
      <button onClick={() => run(() => op(o))} disabled={paused || busy}>
        {label}
      </button>
    )

  return (
    <aside className="drawer host-drawer" aria-label={`host ${host.id}`}>
      <div className="drawer-head">
        <h3>{host.id}</h3>
        <span className={`lifecycle lc-${host.lifecycle}`}>{host.lifecycle}</span>
        <div className="spacer" />
        <button onClick={onClose} aria-label="close">
          ×
        </button>
      </div>
      {paused && <p className="muted">Paused: actions are disabled.</p>}
      <Inventory host={host} />
      <div className="section">
        <h4>Lifecycle</h4>
        <div className="actions">
          {lifecycleButton('Cordon', host.lifecycle === 'Ready', { Cordon: { id: host.id } })}
          <button
            onClick={() => run(() => op({ Drain: { host: host.id } }))}
            disabled={paused || busy || (host.lifecycle !== 'Ready' && host.lifecycle !== 'Cordoned')}
            title="Live-migrate VMs, reschedule pods and move volume replicas off this host"
          >
            Drain
          </button>
          {lifecycleButton('Uncordon', host.lifecycle === 'Cordoned' || host.lifecycle === 'Drained', { Uncordon: { id: host.id } })}
          {lifecycleButton('Cancel drain', host.lifecycle === 'Draining', { Uncordon: { id: host.id } })}
          {lifecycleButton('Remove', host.lifecycle !== 'Removed', { RemoveHost: { id: host.id } })}
        </div>
        {error && <div className="bad">{error}</div>}
      </div>
      <HostWorkloads host={host} onSelectWorkload={onSelectWorkload} />
      {/* Re-mount the editor when the server changes the roles or lifecycle under it. */}
      <RolesEditor key={`${host.id}:${host.lifecycle}:${host.roles.join()}`} host={host} disabled={paused} />
      {/* Re-mount the form when the server changes the config under it, so it does not show a stale draft. */}
      <NetworkEditor key={`${host.id}:${host.net.desired_generation}:${host.override_ !== null}`} host={host} />
    </aside>
  )
}
