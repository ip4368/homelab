import { useMemo, useState } from 'react'
import { op } from '../api'
import type { HostNet } from '../gen/HostNet'
import type { HostObj } from '../gen/HostObj'
import type { NicState } from '../gen/NicState'
import type { Role } from '../gen/Role'
import { podTone, podsOn, SYSTEM_NAMESPACES } from '../kube'
import { useStore } from '../store'
import { hostWorkloads, removeHostPrompt, replicaStateLabel, type WorkloadRef } from '../workloads'
import { Charts } from './Charts'
import { NetworkEditor } from './NetworkEditor'
import { RolePicker } from './RolePicker'
import { useAction } from './useAction'

/** The ports some link of the host's applied (else desired) network config uses; `null` with no config known. */
export function linkPorts(net: Pick<HostNet, 'applied' | 'desired'>): Set<string> | null {
  const cfg = net.applied ?? net.desired
  if (!cfg) return null
  return new Set(cfg.links.flatMap((l) => ('Single' in l.kind ? [l.kind.Single] : l.kind.Bond.members)))
}

/** How a NIC shows: a port down that no link uses (`used`) is just unused, not a fault. */
export function nicState(nic: NicState, used: Set<string> | null): 'up' | 'down' | 'unused' {
  if (nic.link_up) return 'up'
  return used && !used.has(nic.name) ? 'unused' : 'down'
}

const NIC_CLASS = { up: '', down: 'bad', unused: 'muted' } as const

function sameRoles(a: Role[], b: Role[]): boolean {
  return a.length === b.length && a.every((r) => b.includes(r))
}

function Inventory({ host }: { host: HostObj }) {
  const { profile, inventory } = host
  const used = linkPorts(host.net)
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
        {inventory.nics.map((n) => {
          const state = nicState(n, used)
          return (
            <div key={n.name} className={NIC_CLASS[state]}>
              {n.name} {n.speed_mbps} Mb/s {state}
            </div>
          )
        })}
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

const MAX_LISTED = 12

/** One line on the host's Kubernetes node, from what its agent reports. */
export function kubeSummary(k: HostObj['kube']): { text: string; tone: 'ok' | 'warn' | 'bad' | 'muted' } {
  if (!k) return { text: 'not installed yet (comes once the host is Ready on its network)', tone: 'muted' }
  const role = k.role === 'Server' ? 'K3s server (API + etcd)' : k.role === 'Agent' ? 'K3s agent' : 'nothing installed'
  const phase = k.phase
  if (phase === 'Absent') return { text: 'nothing installed', tone: 'muted' }
  if (typeof phase === 'object' && 'Failed' in phase) return { text: `${role}: failed (${phase.Failed}); retrying`, tone: 'bad' }
  if (phase === 'Installing') return { text: `${role}: ${k.detail ?? 'installing'}`, tone: 'warn' }
  const disks = k.storage_disks.length ? `, Longhorn on ${k.storage_disks.join(', ')}` : ''
  return { text: `${role} ${k.version}, node ${k.node_ready ? 'Ready' : 'not Ready'}${disks}`, tone: k.node_ready ? 'ok' : 'warn' }
}

function Kubernetes({ host }: { host: HostObj }) {
  const { text, tone } = kubeSummary(host.kube)
  const kube = useStore((s) => s.kube)
  const pods = useMemo(() => podsOn(kube, host.id), [kube, host.id])
  // Your workloads first; K3s's and Longhorn's own pods are summed up.
  const apps = pods.filter((p) => !SYSTEM_NAMESPACES.includes(p.namespace))
  const system = pods.length - apps.length
  return (
    <div className="section">
      <h4>Kubernetes</h4>
      <p className={tone}>{text}</p>
      {pods.length > 0 && (
        <p>
          {apps.slice(0, MAX_LISTED).map((p, n) => (
            <span key={`${p.namespace}/${p.name}`} title={`${p.namespace}/${p.name}: ${p.status}`}>
              {n > 0 && ', '}
              <span className={podTone(p)}>{p.name}</span>
            </span>
          ))}
          {apps.length > MAX_LISTED && <span className="muted"> and {apps.length - MAX_LISTED} more</span>}
          {system > 0 && (
            <span className="muted">
              {apps.length > 0 ? ', and ' : ''}
              {system} K3s and Longhorn pod{system === 1 ? '' : 's'}
            </span>
          )}
        </p>
      )}
    </div>
  )
}

/** Roles editor: Approve while the host is Pending, otherwise Apply (SetRoles). */
function RolesEditor({ host, disabled = false }: { host: HostObj; disabled?: boolean }) {
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
  const pods = useStore((s) => s.view.pods)
  const vmStatus = useStore((s) => s.view.vmStatus)
  const volumeStatus = useStore((s) => s.view.volumeStatus)
  const { error, busy, run } = useAction()
  if (!host) return null

  const remove = () => {
    // Removal is permanent for the session: say what it does to what runs here.
    if (window.confirm(removeHostPrompt(host.id, hostWorkloads({ pods, vmStatus, volumeStatus }, host.id)))) {
      run(() => op({ RemoveHost: { id: host.id } }))
    }
  }

  const lifecycleButton = (label: string, show: boolean, o: Parameters<typeof op>[0]) =>
    show && (
      <button onClick={() => run(() => op(o))} disabled={busy}>
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
      <Inventory host={host} />
      <Kubernetes host={host} />
      <div className="section">
        <h4>Lifecycle</h4>
        <div className="actions">
          {lifecycleButton('Cordon', host.lifecycle === 'Ready', { Cordon: { id: host.id } })}
          <button
            onClick={() => run(() => op({ Drain: { host: host.id } }))}
            disabled={busy || (host.lifecycle !== 'Ready' && host.lifecycle !== 'Cordoned')}
            title="Live-migrate VMs, reschedule pods and move volume replicas off this host"
          >
            Drain
          </button>
          {lifecycleButton('Uncordon', host.lifecycle === 'Cordoned' || host.lifecycle === 'Drained', { Uncordon: { id: host.id } })}
          {lifecycleButton('Cancel drain', host.lifecycle === 'Draining', { Uncordon: { id: host.id } })}
          {host.lifecycle !== 'Removed' && (
            <button onClick={remove} disabled={busy}>
              Remove
            </button>
          )}
        </div>
        {error && <div className="bad">{error}</div>}
      </div>
      <div className="section">
        <Charts entity={`host:${host.id}`} title="Metrics" />
      </div>
      <HostWorkloads host={host} onSelectWorkload={onSelectWorkload} />
      {/* Re-mount the editor when the server changes the roles or lifecycle under it. */}
      <RolesEditor key={`${host.id}:${host.lifecycle}:${host.roles.join()}`} host={host} />
      {/* Re-mount the form when the server changes the config under it, so it does not show a stale draft. */}
      <NetworkEditor key={`${host.id}:${host.net.desired_generation}:${host.override_ !== null}`} host={host} />
    </aside>
  )
}
