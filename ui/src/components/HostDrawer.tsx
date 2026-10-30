import { useMemo, useState } from 'react'
import { op } from '../api'
import { ineligibleReason } from '../eligibility'
import type { HostNet } from '../gen/HostNet'
import type { HostObj } from '../gen/HostObj'
import type { KubeHealth } from '../gen/KubeHealth'
import type { KubeRepairAction } from '../gen/KubeRepairAction'
import type { KubeRepairAttempt } from '../gen/KubeRepairAttempt'
import type { KubeRole } from '../gen/KubeRole'
import type { NicState } from '../gen/NicState'
import type { Role } from '../gen/Role'
import { age, podTone, podsOn, podsToDrain, removeHostPrompt, SYSTEM_NAMESPACES } from '../kube'
import { useStore } from '../store'
import { Charts } from './Charts'
import { HandoffDialog, handoffChoices } from './HandoffDialog'
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
  const name = (r: KubeRole | null) => (r === 'Server' ? 'K3s server (API + etcd)' : r === 'Agent' ? 'K3s agent' : 'nothing installed')
  const role = name(k.role)
  const phase = k.phase
  if (phase === 'Absent') return { text: 'nothing installed', tone: 'muted' }
  if (typeof phase === 'object' && 'Failed' in phase) return { text: `${role}: failed (${phase.Failed}); retrying`, tone: 'bad' }
  if (phase === 'Installing') {
    // While K3s reinstalls, `role` is still the one installed, and the detail names the one coming:
    // "K3s server (API + etcd): installing K3s as Agent" reads as a server installing an agent.
    const coming = k.detail?.match(/^installing K3s as (Server|Agent)$/)?.[1] as KubeRole | undefined
    if (coming && k.role && coming !== k.role) return { text: `switching from ${role} to ${name(coming)}`, tone: 'warn' }
    if (coming && !k.role) return { text: `installing ${name(coming)}`, tone: 'warn' }
    return { text: `${role}: ${k.detail ?? 'installing'}`, tone: 'warn' }
  }
  const disks = k.storage_disks.length ? `, Longhorn on ${k.storage_disks.join(', ')}` : ''
  const etcd = k.etcd_unread ? `; cannot read etcd's members (${k.etcd_unread})` : ''
  return { text: `${role} ${k.version}, Node ${k.node_ready ? 'Ready' : 'not Ready'}${disks}${etcd}`, tone: k.node_ready && !etcd ? 'ok' : 'warn' }
}

/** A K3s server's own API, from its health reports; `null` for a host with no server. */
export function apiHealthLine(role: KubeRole | null, health: KubeHealth | undefined): { text: string; tone: 'ok' | 'bad' | 'muted' } | null {
  if (role !== 'Server') return null
  if (!health) return { text: 'health unknown (no report yet)', tone: 'muted' }
  return health.api_ready ? { text: 'ready', tone: 'ok' } : { text: 'not ready', tone: 'bad' }
}

/** A repair attempt in words: `restarted K3s 12m ago`. */
export function repairLabel(a: KubeRepairAttempt, now: number): string {
  const what = (action: KubeRepairAction): string => (action === 'RestartK3s' ? 'restarted K3s' : action)
  return `${what(a.action)} ${age(a.at, now)} ago`
}

function Kubernetes({ host }: { host: HostObj }) {
  const { text, tone } = kubeSummary(host.kube)
  const kube = useStore((s) => s.kube)
  const health = useStore((s) => s.view.kube_health[host.id])
  const now = useStore((s) => s.clock.now)
  const pods = useMemo(() => podsOn(kube, host.id), [kube, host.id])
  // Your workloads first; K3s's and Longhorn's own pods are summed up.
  const apps = pods.filter((p) => !SYSTEM_NAMESPACES.includes(p.namespace))
  const system = pods.length - apps.length
  const left = podsToDrain(pods).length
  const api = apiHealthLine(host.kube?.role ?? null, health)
  return (
    <div className="section">
      <h4>Kubernetes</h4>
      <p className={tone}>{text}</p>
      {api && (
        <p>
          K3s API: <span className={api.tone}>{api.text}</span>
        </p>
      )}
      {host.kube_repairs.length > 0 && (
        <p>
          Repairs:{' '}
          {host.kube_repairs
            .map((a) => repairLabel(a, now))
            .join(', ')}
        </p>
      )}
      {host.lifecycle === 'Draining' && (
        <p className="drain-progress" role="status">
          Draining: node cordoned, {left} pod{left === 1 ? '' : 's'} left to evict.
        </p>
      )}
      {host.lifecycle === 'Drained' && <p className="ok">Drained: node cordoned, only DaemonSet pods run here.</p>}
      {host.lifecycle === 'Cordoned' && <p className="muted">Cordoned: no new pods are scheduled here.</p>}
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

function ControlPlane({ host }: { host: HostObj }) {
  const cp = useStore((s) => s.view.cp)
  const hosts = useStore((s) => s.view.hosts)
  const leader = useStore((s) => s.view.leader)
  const [open, setOpen] = useState(false)
  const { error, busy, run } = useAction()
  const voter = cp.voters.includes(host.id)
  const leads = leader === host.id
  const handoff = cp.handoff
  const reason = ineligibleReason(host.profile, 'ControlPlane')
  const offered = useMemo(() => handoffChoices(host, cp, hosts).length > 0, [host, cp, hosts])
  return (
    <div className="section">
      <h4>Control plane</h4>
      <p className={voter ? 'ok' : 'muted'}>
        {voter ? 'Voter: runs a control-plane replica and a K3s server (API + etcd).' : reason ? `Cannot join: ${reason}.` : 'Not a voter.'}
      </p>
      {voter && <p className={leads ? 'ok' : 'muted'}>{leads ? 'Leads: holds the VIP.' : `Follows${leader ? ` ${leader}` : ''}.`}</p>}
      {handoff && (
        <p className="warn" role="status">
          {handoff.cancelled ? 'Cancelling the hand-off of' : 'Handing'} {handoff.from}'s place to {handoff.to}.
        </p>
      )}
      {(handoff || offered) && (
        <div className="actions">
          <button onClick={() => setOpen(true)}>{handoff ? 'Show progress' : voter ? 'Hand off…' : 'Take over…'}</button>
        </div>
      )}
      {voter && !leads && (
        <div className="actions">
          <button
            onClick={() => run(() => op({ TransferLeadership: { to: host.id } }))}
            disabled={busy}
            title="Hand Raft leadership — and with it the VIP — to this voter"
          >
            Move VIP here
          </button>
          {error && <span className="bad">{error}</span>}
        </div>
      )}
      {open && <HandoffDialog host={host} onClose={() => setOpen(false)} />}
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

export function HostDrawer({ id, onClose }: { id: string; onClose(): void }) {
  const host = useStore((s) => s.view.hosts[id])
  const kube = useStore((s) => s.kube)
  const { error, busy, run } = useAction()
  if (!host) return null

  const remove = () => {
    // Removal is permanent: say what still runs here.
    if (window.confirm(removeHostPrompt(host.id, podsOn(kube, host.id)))) {
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
        <span className={`lifecycle lc-${host.lifecycle}`}>{host.lifecycle === 'NotReady' ? 'Host NotReady' : host.lifecycle}</span>
        <div className="spacer" />
        <button onClick={onClose} aria-label="close">
          ×
        </button>
      </div>
      <Inventory host={host} />
      <Kubernetes host={host} />
      <ControlPlane host={host} />
      <div className="section">
        <h4>Lifecycle</h4>
        <div className="actions">
          {lifecycleButton('Cordon', host.lifecycle === 'Ready', { Cordon: { id: host.id } })}
          <button
            onClick={() => run(() => op({ Drain: { host: host.id } }))}
            disabled={busy || (host.lifecycle !== 'Ready' && host.lifecycle !== 'Cordoned')}
            title="Cordon this host's Kubernetes node and evict its pods (kubectl drain)"
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
      {/* Re-mount the editor when the server changes the roles or lifecycle under it. */}
      <RolesEditor key={`${host.id}:${host.lifecycle}:${host.roles.join()}`} host={host} />
      {/* Re-mount the form when the server changes the config under it, so it does not show a stale draft. */}
      <NetworkEditor key={`${host.id}:${host.net.desired_generation}:${host.override_ !== null}`} host={host} />
    </aside>
  )
}
