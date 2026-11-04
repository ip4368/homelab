import { useMemo, useState } from 'react'
import { op } from '../api'
import { ineligibleReason } from '../eligibility'
import type { HostNet } from '../gen/HostNet'
import type { HostObj } from '../gen/HostObj'
import type { KubeRole } from '../gen/KubeRole'
import type { NicState } from '../gen/NicState'
import type { Role } from '../gen/Role'
import { podTone, podsOn, podsToDrain, removeHostPrompt, SYSTEM_NAMESPACES } from '../kube'
import { store, useStore, type Store } from '../store'
import { Charts } from './Charts'
import { LabDebug } from './LabDebug'
import { downVoters, HandoffDialog, handoffChoices, isDown } from './HandoffDialog'
import { InspectorTabs } from './InspectorTabs'
import { NetworkEditor } from './NetworkEditor'
import { ReplaceDialog, replaceChoices } from './ReplaceDialog'
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
  return { text: `${role} ${k.version}, node ${k.node_ready ? 'Ready' : 'not Ready'}${disks}${etcd}`, tone: k.node_ready && !etcd ? 'ok' : 'warn' }
}

function Kubernetes({ host }: { host: HostObj }) {
  const { text, tone } = kubeSummary(host.kube)
  const kube = useStore((s) => s.kube)
  const pods = useMemo(() => podsOn(kube, host.id), [kube, host.id])
  // Your workloads first; K3s's and Longhorn's own pods are summed up.
  const apps = pods.filter((p) => !SYSTEM_NAMESPACES.includes(p.namespace))
  const system = pods.length - apps.length
  const left = podsToDrain(pods).length
  return (
    <div className="section">
      <h4>Kubernetes</h4>
      <p className={tone}>{text}</p>
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

/**
 * Where `host` stands for leading the control plane (and holding the VIP):
 * `leads`, `offer` (a voter the leader can hand over to), `down` (a voter
 * that cannot take over now), or `null` for a non-voter or with no leader to ask.
 */
export function leadership(host: HostObj, voters: string[], leader: string | null): 'leads' | 'offer' | 'down' | null {
  if (!voters.includes(host.id) || leader === null) return null
  if (leader === host.id) return 'leads'
  return isDown(host) ? 'down' : 'offer'
}

/** How long (ms) to wait to see a voter lead after asking to move the VIP to it: the leader gives up after 1.5 s, and the view syncs every 0.5 s. */
export const MOVE_VIP_WAIT_MS = 4000

/**
 * Resolves once `s` shows `to` leading. The leader answers a VIP move as soon
 * as it starts and gives up on it quietly, so this rejects, saying who still
 * leads, if `to` has not taken over within `ms`.
 */
export function untilLeads(s: Store, to: string, ms = MOVE_VIP_WAIT_MS): Promise<void> {
  return new Promise((resolve, reject) => {
    const leads = () => s.getState().view.leader === to
    const timer = setTimeout(() => {
      unsubscribe()
      const leader = s.getState().view.leader
      reject(`${to} did not take over${leader ? `: ${leader} still leads` : ''}. Try again, or check that ${to} is healthy.`)
    }, ms)
    const check = () => {
      if (!leads()) return
      clearTimeout(timer)
      unsubscribe()
      resolve()
    }
    const unsubscribe = s.subscribe(check)
    check()
  })
}

function ControlPlane({ host }: { host: HostObj }) {
  const cp = useStore((s) => s.view.cp)
  const hosts = useStore((s) => s.view.hosts)
  const leader = useStore((s) => s.view.leader)
  const [open, setOpen] = useState<'handoff' | 'replace' | null>(null)
  const { error, busy, run } = useAction()
  const lead = leadership(host, cp.voters, leader)
  const voter = cp.voters.includes(host.id)
  const handoff = cp.handoff
  const reason = ineligibleReason(host.profile, 'ControlPlane')
  const offered = useMemo(() => handoffChoices(host, cp, hosts).length > 0, [host, cp, hosts])
  const replaceable = useMemo(() => replaceChoices(host, cp, hosts), [host, cp, hosts])
  const down = downVoters(cp, hosts)
  return (
    <div className="section">
      <h4>Control plane</h4>
      <p className={voter ? 'ok' : 'muted'}>
        {voter ? 'Voter: runs a control-plane replica and a K3s server (API + etcd).' : reason ? `Cannot join: ${reason}.` : 'Not a voter.'}
      </p>
      {lead === 'leads' && <p className="ok">Leader: holds the VIP, answers the API and runs the controllers.</p>}
      {lead === 'offer' && (
        <div className="actions">
          <button
            onClick={() =>
              run(async () => {
                await op({ TransferCpLeadership: { to: host.id } })
                await untilLeads(store, host.id)
              })
            }
            disabled={busy}
            title={`${leader} hands leadership, and the VIP, to ${host.id}`}
          >
            Move the VIP here
          </button>
          {error && <span className="bad">{error}</span>}
        </div>
      )}
      {handoff && (
        <p className="warn" role="status">
          {handoff.cancelled ? 'Cancelling the hand-off of' : 'Handing'} {handoff.from}'s place to {handoff.to}.
        </p>
      )}
      {voter && isDown(host) && (
        <p className="bad">
          Down: the control plane runs a voter short until {host.id} is replaced
          {replaceable.length === 0 ? '. No Ready, control-plane-eligible host is free to take its place.' : '.'}
        </p>
      )}
      {down.length > 0 && !down.includes(host.id) && (voter || replaceable.length > 0) && (
        <p className="warn">Voter {down.join(', ')} is down: replace it before handing off a control-plane place.</p>
      )}
      {replaceable.length > 0 && !handoff && (
        <div className="actions">
          <button onClick={() => setOpen('replace')}>{voter ? 'Replace…' : `Replace ${replaceable.length === 1 ? replaceable[0].dead : 'a down voter'}…`}</button>
        </div>
      )}
      {(handoff || offered) && (
        <div className="actions">
          <button onClick={() => setOpen('handoff')}>{handoff ? 'Show progress' : voter ? 'Hand off…' : 'Take over…'}</button>
        </div>
      )}
      {open === 'handoff' && <HandoffDialog host={host} onClose={() => setOpen(null)} />}
      {open === 'replace' && <ReplaceDialog host={host} onClose={() => setOpen(null)} />}
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

const HOST_TABS = ['Overview', 'Metrics', 'Operate', 'Network', 'Debug'] as const
type HostTab = (typeof HOST_TABS)[number]

export function HostDrawer({ id, onClose }: { id: string; onClose(): void }) {
  const host = useStore((s) => s.view.hosts[id])
  const kube = useStore((s) => s.kube)
  const metric = useStore((s) => s.view.metrics[id])
  const { error, busy, run } = useAction()
  const [tab, setTab] = useState<HostTab>('Overview')
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
        <span className={`lifecycle lc-${host.lifecycle}`}>{host.lifecycle}</span>
        <div className="spacer" />
        <button onClick={onClose} aria-label="close">
          ×
        </button>
      </div>
      <div className="inspector-summary">
        <span><b>{host.profile.id}</b><small>{host.location}</small></span>
        <span title={host.roles.join(', ')}><b>{host.roles.length ? `${host.roles.length} roles` : 'No roles'}</b><small>{host.roles.slice(0, 2).join(', ') || 'none assigned'}{host.roles.length > 2 ? '…' : ''}</small></span>
        <span><b>{metric ? `${metric.cpu_pct.toFixed(0)}% CPU` : 'No sample'}</b><small>{metric ? `${metric.ram_pct.toFixed(0)}% RAM` : 'metrics pending'}</small></span>
        <span><b>{podsOn(kube, host.id).length}</b><small>pods here</small></span>
      </div>
      <InspectorTabs tabs={HOST_TABS} value={tab} onChange={(next) => setTab(next)} label={`Host ${host.id} sections`} />
      <div className="inspector-body">
      {tab === 'Overview' && (
        <>
          <Inventory host={host} />
          <Kubernetes host={host} />
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
        </>
      )}
      {tab === 'Metrics' && (
        <div className="section inspector-charts">
          <Charts entity={`host:${host.id}`} title="Metrics" row />
        </div>
      )}
      {tab === 'Operate' && (
        <>
          <ControlPlane host={host} />
          <RolesEditor key={`${host.id}:${host.lifecycle}:${host.roles.join()}`} host={host} />
        </>
      )}
      {tab === 'Network' && <NetworkEditor key={`${host.id}:${host.net.desired_generation}:${host.override_ !== null}`} host={host} />}
      {tab === 'Debug' && <LabDebug host={host} />}
      </div>
    </aside>
  )
}
