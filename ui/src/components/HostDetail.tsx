import { useMemo, useState } from 'react'
import { op } from '../api'
import { ineligibleReason } from '../eligibility'
import type { HostNet } from '../gen/HostNet'
import type { HostObj } from '../gen/HostObj'
import type { KubeRole } from '../gen/KubeRole'
import type { NicState } from '../gen/NicState'
import type { Role } from '../gen/Role'
import type { KubePod } from '../gen/KubePod'
import { age, cpu, mem, podTone, podWhy, podsOn, podsToDrain, removeHostPrompt, SYSTEM_NAMESPACES } from '../kube'
import { nav } from '../nav'
import { store, useStore, type Store } from '../store'
import { Charts } from './Charts'
import { Card, DetailPane, type PaneTab } from './DetailPane'
import { Usage } from './KubeBits'
import { MachineControls } from './LabPage'
import { Menu } from './Menu'
import { PodButtons } from './PodButtons'
import { downVoters, HandoffDialog, handoffChoices, isDown } from './HandoffDialog'
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

function Kubernetes({ host, pods, onPods }: { host: HostObj; pods: KubePod[]; onPods(): void }) {
  const { text, tone } = kubeSummary(host.kube)
  // Your workloads first; K3s's and Longhorn's own pods are summed up.
  const apps = pods.filter((p) => !SYSTEM_NAMESPACES.includes(p.namespace))
  const system = pods.length - apps.length
  const left = podsToDrain(pods).length
  const troubled = pods.filter((p) => podTone(p) === 'bad' || podTone(p) === 'warn').length
  return (
    <Card title="Kubernetes">
      <p className={tone}>{text}</p>
      {host.lifecycle === 'Draining' && (
        <p className="drain-progress" role="status">
          Draining: node cordoned, {left} pod{left === 1 ? '' : 's'} left to evict.
        </p>
      )}
      {host.lifecycle === 'Drained' && <p className="ok">Drained: node cordoned, only DaemonSet pods run here.</p>}
      {host.lifecycle === 'Cordoned' && <p className="muted">Cordoned: no new pods are scheduled here.</p>}
      <div className="kpis">
        <button className="kpi" onClick={onPods}>
          <span className="kpi-value">{apps.length}</span>
          <span className="kpi-label">your pods</span>
        </button>
        <button className="kpi" onClick={onPods}>
          <span className="kpi-value muted">{system}</span>
          <span className="kpi-label">K3s and Longhorn pods</span>
        </button>
        <button className="kpi" onClick={onPods}>
          <span className={`kpi-value ${troubled ? 'warn' : 'muted'}`}>{troubled}</span>
          <span className="kpi-label">not running well</span>
        </button>
      </div>
    </Card>
  )
}

/** The pods on a host, yours first, with their logs and shells. */
function HostPods({ pods }: { pods: KubePod[] }) {
  const now = useStore((s) => s.clock.now)
  const [system, setSystem] = useState(false)
  const shown = useMemo(
    () =>
      pods
        .filter((p) => system || !SYSTEM_NAMESPACES.includes(p.namespace))
        .sort((a, b) => Number(SYSTEM_NAMESPACES.includes(a.namespace)) - Number(SYSTEM_NAMESPACES.includes(b.namespace)) || a.namespace.localeCompare(b.namespace) || a.name.localeCompare(b.name)),
    [pods, system],
  )
  const hidden = pods.length - pods.filter((p) => !SYSTEM_NAMESPACES.includes(p.namespace)).length
  return (
    <>
      <div className="toolbar">
        <label className="inline">
          <input type="checkbox" checked={system} onChange={(e) => setSystem(e.target.checked)} /> show K3s, Longhorn and KubeVirt ({hidden})
        </label>
      </div>
      <div className="table-wrap">
        <table className="grid-table">
          <thead>
            <tr>
              <th>Pod</th>
              <th>Status</th>
              <th className="num">CPU</th>
              <th className="num">Memory</th>
              <th className="num hide-narrow">Age</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {shown.map((p) => {
              const owner = p.owner ? `${p.namespace}/${p.owner}` : null
              return (
                <tr key={`${p.namespace}/${p.name}`}>
                  <td>
                    <div className="cell-title">{p.name}</div>
                    <div className="cell-sub">
                      {p.namespace}
                      {owner && (
                        <>
                          {' · '}
                          <button className="link" onClick={() => nav.workload(owner)}>
                            {p.owner}
                          </button>
                        </>
                      )}
                    </div>
                  </td>
                  <td>
                    <span className={`pill ${podTone(p)}`} title={podWhy(p)}>
                      {p.status}
                    </span>
                    {p.restarts > 0 && (
                      <span className="warn">
                        {' '}
                        {p.restarts} restart{p.restarts === 1 ? '' : 's'}
                      </span>
                    )}
                  </td>
                  <td className="num">
                    <Usage used={p.cpu_milli} limit={p.cpu_limit_milli} format={cpu} />
                  </td>
                  <td className="num">
                    <Usage used={p.memory_bytes} limit={p.memory_limit_bytes} format={mem} />
                  </td>
                  <td className="num muted hide-narrow">{age(p.created, now)}</td>
                  <td className="row-actions">
                    <PodButtons pod={p} />
                  </td>
                </tr>
              )
            })}
            {shown.length === 0 && (
              <tr>
                <td colSpan={6} className="muted">
                  No pods{hidden > 0 && !system ? ' of yours' : ''} here.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </>
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
    <Card title="Control plane">
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
    </Card>
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
    <Card title="Roles">
      <p className="muted">What the host runs: control plane, Longhorn storage, workloads, VMs, transcoding. The server refuses roles the hardware cannot take.</p>
      <RolePicker profile={host.profile} value={roles} onChange={setRoles} />
      <div className="actions">
        <button onClick={submit} disabled={disabled || busy || (!pending && sameRoles(roles, host.roles))}>
          {pending ? 'Approve' : 'Apply roles'}
        </button>
        {error && <span className="bad">{error}</span>}
      </div>
    </Card>
  )
}

const LIFECYCLE_TONE: Record<HostObj['lifecycle'], string> = {
  Ready: 'ok',
  NotReady: 'bad',
  Pending: 'warn',
  Approved: 'warn',
  Provisioning: 'warn',
  Cordoned: 'purple',
  Draining: 'purple',
  Drained: 'purple',
  Removed: 'muted',
}

export function lifecycleTone(l: HostObj['lifecycle']): string {
  return LIFECYCLE_TONE[l]
}

/** Cordon, Drain, Uncordon, Cancel drain and Remove, as the host's lifecycle allows. */
function LifecycleActions({ host, pods }: { host: HostObj; pods: KubePod[] }) {
  const { error, busy, run } = useAction()
  const remove = () => {
    // Removal is permanent: say what still runs here.
    if (window.confirm(removeHostPrompt(host.id, pods))) run(() => op({ RemoveHost: { id: host.id } }))
  }
  const l = host.lifecycle
  return (
    <>
      {l === 'Ready' && (
        <button onClick={() => run(() => op({ Cordon: { id: host.id } }))} disabled={busy} title="No new pods are scheduled here">
          Cordon
        </button>
      )}
      {(l === 'Ready' || l === 'Cordoned') && (
        <button onClick={() => run(() => op({ Drain: { host: host.id } }))} disabled={busy} title="Cordon this host's Kubernetes node and evict its pods (kubectl drain)">
          Drain
        </button>
      )}
      {(l === 'Cordoned' || l === 'Drained') && (
        <button onClick={() => run(() => op({ Uncordon: { id: host.id } }))} disabled={busy}>
          Uncordon
        </button>
      )}
      {l === 'Draining' && (
        <button onClick={() => run(() => op({ Uncordon: { id: host.id } }))} disabled={busy}>
          Cancel drain
        </button>
      )}
      {l !== 'Removed' && (
        <Menu
          label={`more actions for ${host.id}`}
          items={[{ label: 'Remove from the cluster…', onClick: remove, danger: true, disabled: busy, title: 'Permanent: the host must join again to come back' }]}
        />
      )}
      {error && <span className="bad">{error}</span>}
    </>
  )
}

/**
 * One host in the detail pane: what it is and runs (hardware, Kubernetes,
 * control plane, charts), its pods, network, roles and, in the lab, its
 * machine's power, cables and disks.
 */
export function HostDetail({ id, tab, onTab, onClose, showTruth }: { id: string; tab: string | null; onTab(t: string): void; onClose(): void; showTruth: boolean }) {
  const host = useStore((s) => s.view.hosts[id])
  const kube = useStore((s) => s.kube)
  const leader = useStore((s) => s.view.leader)
  const voters = useStore((s) => s.view.cp.voters)
  const inLab = useStore((s) => s.truth.vms.some((v) => v.id === id))
  const metric = useStore((s) => s.view.metrics[id])
  const pods = useMemo(() => podsOn(kube, id), [kube, id])
  if (!host) return null
  const apps = pods.filter((p) => !SYSTEM_NAMESPACES.includes(p.namespace)).length
  const tabs: PaneTab[] = [
    { id: 'overview', label: 'Overview' },
    { id: 'pods', label: 'Pods', count: apps },
    { id: 'network', label: 'Network', tone: host.net.state === 'Failed' ? 'bad' : undefined },
    { id: 'roles', label: 'Roles' },
    ...(inLab ? [{ id: 'lab', label: 'Lab machine' }] : []),
  ]
  return (
    <DetailPane
      label={`host ${host.id}`}
      title={host.id}
      status={<span className={`pill ${lifecycleTone(host.lifecycle)}`}>{host.lifecycle}</span>}
      subtitle={
        <>
          {host.profile.id} · {host.location}
          {leader === host.id ? ' · leader, holds the VIP' : voters.includes(host.id) ? ' · control-plane voter' : ''}
          {metric && (
            <span className="muted">
              {' '}
              · CPU {metric.cpu_pct.toFixed(0)}% · RAM {metric.ram_pct.toFixed(0)}%
            </span>
          )}
          <span className="role-badges">
            {host.roles.map((r) => (
              <span key={r} className="badge">
                {r}
              </span>
            ))}
          </span>
        </>
      }
      actions={<LifecycleActions host={host} pods={pods} />}
      tabs={tabs}
      tab={tab}
      onTab={onTab}
      onClose={onClose}
    >
      {(t) =>
        t === 'pods' ? (
          <HostPods pods={pods} />
        ) : t === 'network' ? (
          // Re-mount the form when the server changes the config under it, so it does not show a stale draft.
          <NetworkEditor key={`${host.id}:${host.net.desired_generation}:${host.override_ !== null}`} host={host} />
        ) : t === 'roles' ? (
          // Re-mount the editor when the server changes the roles or lifecycle under it.
          <RolesEditor key={`${host.id}:${host.lifecycle}:${host.roles.join()}`} host={host} />
        ) : t === 'lab' ? (
          <MachineControls vm={host.id} showTruth={showTruth} />
        ) : (
          <div className="card-grid">
            <Card title="Hardware">
              <Inventory host={host} />
            </Card>
            <Kubernetes host={host} pods={pods} onPods={() => onTab('pods')} />
            <ControlPlane host={host} />
            <Card title="Metrics" className="span-all">
              <Charts entity={`host:${host.id}`} title="" />
            </Card>
          </div>
        )
      }
    </DetailPane>
  )
}
