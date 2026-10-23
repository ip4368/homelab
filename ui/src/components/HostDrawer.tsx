import { useState } from 'react'
import { op } from '../api'
import type { HostObj } from '../gen/HostObj'
import type { Role } from '../gen/Role'
import { useStore } from '../store'
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

export function HostDrawer({ id, onClose }: { id: string; onClose(): void }) {
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
      {/* Re-mount the editor when the server changes the roles or lifecycle under it. */}
      <RolesEditor key={`${host.id}:${host.lifecycle}:${host.roles.join()}`} host={host} disabled={paused} />
      {/* Re-mount the form when the server changes the config under it, so it does not show a stale draft. */}
      <NetworkEditor key={`${host.id}:${host.net.desired_generation}:${host.override_ !== null}`} host={host} />
      <div className="section">
        <h4>Lifecycle</h4>
        <div className="actions">
          {lifecycleButton('Cordon', host.lifecycle === 'Ready', { Cordon: { id: host.id } })}
          {lifecycleButton('Uncordon', host.lifecycle === 'Cordoned', { Uncordon: { id: host.id } })}
          {lifecycleButton('Remove', host.lifecycle !== 'Removed', { RemoveHost: { id: host.id } })}
        </div>
        {error && <div className="bad">{error}</div>}
      </div>
    </aside>
  )
}
