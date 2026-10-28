// End-to-end validation of the cluster manager on the real VM lab.
//
// Talks only to the server's public interfaces: the WebSocket (the control
// plane's view and the lab's ground truth), `POST /api/lab/fault` (things
// done to hardware) and `POST /api/ops` (a control-plane replacement).
// Each step is a success criterion of the real VM lab design
// (docs/plans/2026-10-26-real-vm-lab-design.md §10), timed, and the
// Kubernetes criteria K1–K8 of docs/plans/2026-10-27-k3s-longhorn-lab-design.md
// (§5 for K1–K6, §8 for K7–K8), checked against the cluster's Kubernetes API
// through the VIP with the admin kubeconfig `GET /api/lab/kubeconfig` hands
// out (as an admin would get it with `sudo homelabd kubeconfig`).
//
//   node e2e/lab.e2e.ts [--only 3a,4]     (LAB_URL defaults to http://127.0.0.1:8080)
//
// Criterion 2 racks two new MS-01s each time it runs, and the lab keeps
// them; `./lab.sh reset` starts over from the first four.

import type { HostObj } from '../ui/src/gen/HostObj.ts'
import type { LabFault } from '../ui/src/gen/LabFault.ts'
import type { LabTruth } from '../ui/src/gen/LabTruth.ts'
import type { ServerMsg } from '../ui/src/gen/ServerMsg.ts'
import type { Alert } from '../ui/src/gen/Alert.ts'
import type { CpInfo } from '../ui/src/gen/CpInfo.ts'
import type { SwitchObj } from '../ui/src/gen/SwitchObj.ts'
import type { Mistake } from '../ui/src/gen/Mistake.ts'
import type { PortTruth } from '../ui/src/gen/PortTruth.ts'
import type { SwitchPortCfg } from '../ui/src/gen/SwitchPortCfg.ts'
import { Kube } from './kube.ts'

const BASE = process.env.LAB_URL ?? 'http://127.0.0.1:8080'

/** What the e2e run follows of the server's stream. */
interface Seen {
  leader: string | null
  term: number
  hosts: Map<string, HostObj>
  alerts: Map<string, Alert>
  switches: Map<string, SwitchObj>
  cp: CpInfo
  truth: LabTruth
}

class Watcher {
  seen: Seen | null = null
  /** Why the stream ended, once it has: what was seen is frozen from then on. */
  closed: string | null = null
  private ws: WebSocket
  ready: Promise<void>

  constructor() {
    this.ws = new WebSocket(BASE.replace(/^http/, 'ws') + '/api/ws')
    this.ready = new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`no message from ${BASE}/api/ws in 15 s`)), 15_000)
      this.ws.onerror = () => reject(new Error(`cannot open ${BASE}/api/ws`))
      this.ws.onclose = (e) => {
        this.closed = `the server's stream closed (code ${e.code})`
        reject(new Error(this.closed))
      }
      this.ws.onmessage = (e) => {
        this.apply(JSON.parse(String(e.data)) as ServerMsg)
        clearTimeout(timer)
        resolve()
      }
    })
  }

  private apply(m: ServerMsg) {
    if (m.type === 'snapshot') {
      this.seen = {
        leader: m.view.leader,
        term: m.view.term,
        hosts: new Map(m.view.hosts.map((h) => [h.id, h])),
        alerts: new Map(m.view.alerts.map((a) => [a.id, a])),
        switches: new Map(m.view.switches.map((w) => [w.id, w])),
        cp: m.view.cp,
        truth: m.truth,
      }
      return
    }
    const s = this.seen!
    s.leader = m.leader
    s.term = m.term
    s.truth = m.truth
    for (const o of m.upserts) {
      if (o.type === 'Host') s.hosts.set(o.id, o)
      else if (o.type === 'Alert') s.alerts.set(o.id, o)
      else if (o.type === 'Switch') s.switches.set(o.id, o)
      else if (o.type === 'Cp') s.cp = o
    }
    for (const r of m.removed) {
      if (r.kind === 'Host') s.hosts.delete(r.id)
      else if (r.kind === 'Alert') s.alerts.delete(r.id)
      else if (r.kind === 'Switch') s.switches.delete(r.id)
    }
  }

  close() {
    this.ws.onclose = null
    this.ws.close()
  }
}

const watch = new Watcher()
const now = () => Date.now()
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

/** Does `f` to the lab's hardware; the machine `AddVm` racked, if that was it. */
async function fault(f: LabFault): Promise<string | undefined> {
  const res = await fetch(`${BASE}/api/lab/fault`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(f) })
  if (!res.ok) throw new Error(`fault ${JSON.stringify(f)}: ${res.status} ${await res.text()}`)
  return ((await res.json()) as { vm?: string }).vm
}

class Failed extends Error {}

/** Polls until `check` returns something truthy; its value and how long it took. */
async function waitFor<T>(what: string, timeoutMs: number, check: (s: Seen) => T | null | undefined | false): Promise<{ value: T; ms: number }> {
  const start = now()
  for (;;) {
    if (watch.closed) throw new Failed(watch.closed)
    const v = watch.seen && check(watch.seen)
    if (v) return { value: v, ms: now() - start }
    if (now() - start > timeoutMs) throw new Failed(`timed out after ${timeoutMs / 1000} s waiting for ${what}`)
    await sleep(100)
  }
}

function openAlerts(s: Seen, kind: string, entity?: string): Alert[] {
  return [...s.alerts.values()].filter((a) => a.resolved_at === null && a.kind === kind && (entity === undefined || a.entity.id === entity))
}

/** Every machine installed and running, every host Ready, 3 voters, one leader holding the only VIP. Why not, or null. */
function unhealthy(s: Seen): string | null {
  const vms = s.truth.vms
  if (vms.length === 0) return 'no lab machines'
  const off = vms.filter((v) => !v.installed || v.power !== 'Running')
  if (off.length) return `not running: ${off.map((v) => v.id).join(', ')}`
  const notReady = vms.filter((v) => s.hosts.get(v.id)?.lifecycle !== 'Ready')
  if (notReady.length) return `not Ready: ${notReady.map((v) => `${v.id}=${s.hosts.get(v.id)?.lifecycle ?? 'absent'}`).join(', ')}`
  if (s.cp.voters.length !== 3) return `${s.cp.voters.length} voters`
  if (!s.leader) return 'no leader'
  if (s.truth.vip_holders.length !== 1 || s.truth.vip_holders[0] !== s.leader) return `VIP held by [${s.truth.vip_holders}], leader ${s.leader}`
  if (s.truth.split_brain_vms.length) return `split brain: ${s.truth.split_brain_vms}`
  return null
}

async function healthy(timeoutMs: number) {
  let why: string | null = 'not checked'
  try {
    return await waitFor('a healthy cluster', timeoutMs, (s) => (why = unhealthy(s)) === null)
  } catch (e) {
    throw new Failed(`${(e as Error).message} (last: ${why})`)
  }
}

interface Result {
  id: string
  name: string
  ok: boolean
  notes: string[]
}
const results: Result[] = []

async function step(id: string, name: string, fn: (note: (s: string) => void) => Promise<void>) {
  const only = process.argv.includes('--only') ? process.argv[process.argv.indexOf('--only') + 1].split(',') : null
  if (only && !only.includes(id)) return
  const r: Result = { id, name, ok: false, notes: [] }
  const note = (s: string) => {
    r.notes.push(s)
    console.log(`    ${s}`)
  }
  console.log(`\n[${id}] ${name}`)
  try {
    await fn(note)
    r.ok = true
  } catch (e) {
    note(`FAIL: ${(e as Error).message}`)
  }
  results.push(r)
}

const secs = (ms: number) => `${(ms / 1000).toFixed(1)} s`

// ---- Kubernetes ---------------------------------------------------------

let kubeClient: Kube | null = null

/** The cluster's Kubernetes API, with the admin kubeconfig a K3s server prints. */
async function kube(): Promise<Kube> {
  if (kubeClient) return kubeClient
  const deadline = now() + 10 * 60_000
  for (;;) {
    const res = await fetch(`${BASE}/api/lab/kubeconfig`)
    if (res.ok) return (kubeClient = new Kube(await res.text()))
    if (now() > deadline) throw new Failed(`no kubeconfig: ${res.status} ${await res.text()}`)
    await sleep(5000)
  }
}

/** Polls the async `check` until it returns something truthy (or throws `Failed` with the last reason). */
async function waitAsync<T>(what: string, timeoutMs: number, check: () => Promise<T | null | undefined | false | string>): Promise<{ value: T; ms: number }> {
  const start = now()
  let why = ''
  for (;;) {
    let v: T | null | undefined | false | string
    try {
      v = await check()
    } catch (e) {
      v = (e as Error).message
    }
    if (typeof v === 'string') why = v
    else if (v) return { value: v, ms: now() - start }
    if (now() - start > timeoutMs) throw new Failed(`timed out after ${timeoutMs / 1000} s waiting for ${what}${why ? ` (last: ${why})` : ''}`)
    await sleep(2000)
  }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Obj = any
const isReady = (n: Obj) => n.status?.conditions?.some((c: Obj) => c.type === 'Ready' && c.status === 'True') ?? false
const LH = '/apis/longhorn.io/v1beta2/namespaces/longhorn-system'

/** Every lab machine a Ready node, the K3s servers exactly the CP voters, Longhorn running with a schedulable disk per data disk on every storage host. Why not, or the nodes. */
async function kubeUnhealthy(k: Kube): Promise<string | Obj[]> {
  const s = watch.seen!
  const nodes: Obj[] = (await k.must('/api/v1/nodes')).items
  const byName = new Map(nodes.map((n) => [n.metadata.name, n]))
  const vms = s.truth.vms.filter((v) => v.installed)
  const missing = vms.filter((v) => !byName.has(v.id) || !isReady(byName.get(v.id)))
  if (missing.length) return `not Ready nodes: ${missing.map((v) => v.id).join(', ')}`
  const servers = nodes.filter((n) => n.metadata.labels['node-role.kubernetes.io/control-plane'] === 'true').map((n) => n.metadata.name).sort()
  if (servers.join() !== [...s.cp.voters].sort().join()) return `K3s servers [${servers}] are not the voters [${s.cp.voters}]`
  const lhNodes = await k.get(`${LH}/nodes`)
  if (lhNodes.status !== 200) return `Longhorn not installed yet (${lhNodes.status})`
  const lh = new Map<string, Obj>(lhNodes.body.items.map((n: Obj) => [n.metadata.name, n]))
  for (const h of s.hosts.values()) {
    if (!h.roles.includes('Storage') || h.net.held_roles.Storage !== undefined || h.lifecycle !== 'Ready') continue
    const n = lh.get(h.id)
    if (!n) return `no Longhorn node ${h.id}`
    for (const d of h.inventory.disks.filter((d) => !d.failed)) {
      const disk = Object.entries(n.spec.disks ?? {}).find(([, v]: [string, Obj]) => v.path === `/mnt/${d.name}`)
      if (!disk) return `Longhorn has no disk /mnt/${d.name} on ${h.id}`
      const st = n.status?.diskStatus?.[disk[0]]
      if (!st?.conditions?.some((c: Obj) => c.type === 'Schedulable' && c.status === 'True')) return `disk /mnt/${d.name} on ${h.id} not schedulable`
    }
  }
  const pods: Obj[] = (await k.must('/api/v1/namespaces/longhorn-system/pods')).items
  const down = pods.filter((p) => !['Running', 'Succeeded'].includes(p.status.phase))
  if (down.length) return `Longhorn pods not running: ${down.map((p) => `${p.metadata.name}=${p.status.phase}`).join(', ')}`
  return nodes
}

async function kubeHealthy(timeoutMs: number) {
  const k = await kube()
  return waitAsync('a healthy Kubernetes', timeoutMs, async () => {
    const r = await kubeUnhealthy(k)
    return typeof r === 'string' ? r : r
  })
}

await watch.ready

await step('1', 'Up: every machine Ready, three CP voters, one VIP holder', async (note) => {
  const { ms } = await healthy(15 * 60_000)
  const s = watch.seen!
  note(`healthy after ${secs(ms)}: leader ${s.leader} (term ${s.term}), voters ${s.cp.voters.join(', ')}`)
  note(`clock offsets from the lab host: ${s.truth.vms.map((v) => `${v.id} ${v.clock_offset_ms ?? '?'} ms`).join(', ')}`)
  const unmeasured = s.truth.vms.filter((v) => v.clock_offset_ms === null)
  if (unmeasured.length) throw new Failed(`no clock reading from ${unmeasured.map((v) => v.id).join(', ')} (guest agent not answering)`)
  if (Math.max(...s.truth.vms.map((v) => Math.abs(v.clock_offset_ms!))) > 250) throw new Failed('guest clocks disagree by more than 250 ms')
})

await step('K1', 'Kubernetes: every host a Ready node, servers on the CP voters, Longhorn on the storage hosts\' data disks', async (note) => {
  await healthy(5 * 60_000)
  const { value: nodes, ms } = await kubeHealthy(20 * 60_000)
  const s = watch.seen!
  const servers = nodes.filter((n: Obj) => n.metadata.labels['node-role.kubernetes.io/control-plane'] === 'true').map((n: Obj) => n.metadata.name)
  note(`healthy after ${secs(ms)}: ${nodes.length} Ready nodes (${nodes.map((n: Obj) => `${n.metadata.name} ${n.status.nodeInfo.kubeletVersion}`).join(', ')})`)
  note(`K3s servers (etcd): ${servers.join(', ')} = CP voters`)
  const notRunning = [...s.hosts.values()].filter((h) => h.kube?.phase !== 'Running')
  if (notRunning.length) throw new Failed(`homelabd does not report Kubernetes Running on ${notRunning.map((h) => h.id).join(', ')}`)
  const lh: Obj[] = (await (await kube()).must(`${LH}/nodes`)).items
  note(`Longhorn disks: ${lh.map((n) => `${n.metadata.name} [${Object.values(n.spec.disks ?? {}).map((d: Obj) => d.path).join(' ')}]`).join(', ')}`)
  const api = await (await kube()).get('/readyz')
  if (api.status !== 200) throw new Failed(`the API through the VIP answered ${api.status}`)
  note(`the API answers through the VIP (${(await kube()).server.host})`)
})

/** Racks a new MS-01 (with `mistake`, if any) and waits until it is a Ready host; its id and how long that took. */
async function addMs01(mistake: Mistake | null): Promise<{ id: string; ms: number }> {
  const start = now()
  const id = await fault({ AddVm: { profile: 'Ms01', mistake } })
  if (!id) throw new Failed('AddVm named no machine')
  await waitFor(`${id} Ready with its network applied`, 15 * 60_000, (s) => {
    const h = s.hosts.get(id)
    return h?.lifecycle === 'Ready' && h.net.state === 'Applied' && h.lldp.length > 0
  })
  return { id, ms: now() - start }
}

await step('2', 'Join: Add VM boots a fifth machine that becomes Ready unaided; a technician mistake is flagged', async (note) => {
  await healthy(5 * 60_000)
  const clean = await addMs01(null)
  const h = watch.seen!.hosts.get(clean.id)!
  note(`racked ${clean.id}: Ready, template ${h.template}, roles ${h.roles.join(', ')}, network Applied ${secs(clean.ms)} after the request`)
  // Its switch ports are judged at the next poll: wait one out before calling it clean.
  await sleep(35_000)
  if (openAlerts(watch.seen!, 'NetworkMismatch', clean.id).length) throw new Failed(`${clean.id} has a network mismatch`)
  if (Object.keys(h.net.held_roles).length) throw new Failed(`${clean.id} holds roles inactive: ${JSON.stringify(h.net.held_roles)}`)

  const bad = await addMs01('MissingStorageVlan')
  note(`racked ${bad.id} with VLAN 20 left off its trunk: Ready ${secs(bad.ms)} after the request`)
  const flagged = await waitFor(`a NetworkMismatch alert on ${bad.id} and its storage role held`, 120_000, (s) => {
    const a = openAlerts(s, 'NetworkMismatch', bad.id)[0]
    return a && s.hosts.get(bad.id)?.net.held_roles.Storage !== undefined && a
  })
  note(`flagged: "${flagged.value.message}"; storage role held inactive (needs ${watch.seen!.hosts.get(bad.id)!.net.held_roles.Storage})`)

  // K2: both join Kubernetes; only the clean one gives Longhorn its disks.
  const k = await kube()
  const joined = await kubeHealthy(15 * 60_000)
  note(`K2: ${clean.id} and ${bad.id} are Ready Kubernetes nodes ${secs(joined.ms)} later`)
  const lh = new Map<string, Obj>(((await k.must(`${LH}/nodes`)).items as Obj[]).map((n) => [n.metadata.name, n]))
  const disks = (id: string) => Object.values(lh.get(id)?.spec.disks ?? {}).map((d: Obj) => d.path)
  if (disks(clean.id).length !== 2) throw new Failed(`${clean.id} gave Longhorn ${disks(clean.id).length} disks`)
  if (disks(bad.id).length !== 0) throw new Failed(`${bad.id}, its storage role held, gave Longhorn disks: ${disks(bad.id)}`)
  note(`K2: ${clean.id} stores on ${disks(clean.id).join(' ')}; ${bad.id} (storage held) gives Longhorn no disks`)
})

await step('3a', 'Leader failover: power off the leader', async (note) => {
  await healthy(5 * 60_000)
  const old = watch.seen!.leader!
  const term = watch.seen!.term
  const k = await kube()
  await fault({ PowerOff: { vm: old } })
  const offAt = now()
  const elected = await waitFor('a new leader in the client view', 30_000, (s) => s.leader !== null && s.leader !== old && s.leader)
  note(`powered off ${old}; the client saw ${elected.value} lead (term ${watch.seen!.term} > ${term}) after ${secs(elected.ms)}`)
  // K5: the Kubernetes API (a K3s server on the new leader) answers through the VIP again.
  const api = await waitAsync('the Kubernetes API through the VIP', 60_000, async () => {
    const r = await k.get('/readyz')
    return r.status === 200 || `readyz ${r.status} ${r.text.slice(0, 80)}`
  })
  note(`K5: the Kubernetes API answered through the VIP ${secs(now() - offAt)} after the power-off`)
  if (now() - offAt > 30_000) throw new Failed(`the Kubernetes API was gone for ${secs(now() - offAt)}`)
  void api
  if (elected.ms > 10_000) throw new Failed(`re-election took ${secs(elected.ms)} (2 election timeouts are at most 6 s, plus reconnecting through the VIP)`)
  const vip = await waitFor('the VIP on the new leader only', 15_000, (s) => s.truth.vip_holders.length === 1 && s.truth.vip_holders[0] === elected.value)
  note(`the VIP moved to ${elected.value} (${secs(elected.ms + vip.ms)} after the power-off)`)
  const down = await waitFor(`${old} NotReady with a HostDown alert`, 90_000, (s) => s.hosts.get(old)?.lifecycle === 'NotReady' && openAlerts(s, 'HostDown', old).length > 0)
  note(`${old} marked NotReady, HostDown raised, ${secs(elected.ms + vip.ms + down.ms)} after the power-off`)
  await fault({ PowerOn: { vm: old } })
  const back = await healthy(5 * 60_000)
  note(`${old} powered on and back as a Ready follower after ${secs(back.ms)}`)
  if (watch.seen!.leader !== elected.value) throw new Failed(`the returning voter disrupted the leader: now ${watch.seen!.leader}`)
  note(`the leader stayed ${elected.value}: the returning voter did not disrupt it`)
})

await step('3b', 'Stale leader: freeze the leader, then thaw it', async (note) => {
  await healthy(5 * 60_000)
  const old = watch.seen!.leader!
  await fault({ Freeze: { vm: old } })
  const elected = await waitFor('a new leader in the client view', 30_000, (s) => s.leader !== null && s.leader !== old && s.leader)
  note(`froze ${old}; ${elected.value} leads after ${secs(elected.ms)}`)
  await waitFor(`${old} NotReady`, 90_000, (s) => s.hosts.get(old)?.lifecycle === 'NotReady')
  note(`${old} NotReady while frozen (its VIP address, as last seen: ${watch.seen!.truth.vms.find((v) => v.id === old)?.holds_vip})`)
  await fault({ Thaw: { vm: old } })
  const thawed = now()
  let doubleSince: number | null = null
  let longestDouble = 0
  let leaderChanges = 0
  let lastLeader = watch.seen!.leader
  while (now() - thawed < 120_000) {
    const s = watch.seen!
    if (s.truth.vip_holders.length > 1) {
      doubleSince ??= now()
      longestDouble = Math.max(longestDouble, now() - doubleSince)
    } else {
      doubleSince = null
    }
    if (s.leader !== lastLeader) {
      leaderChanges++
      lastLeader = s.leader
    }
    if (unhealthy(s) === null && now() - thawed > 3000) break
    await sleep(100)
  }
  note(`thawed ${old}: healthy again after ${secs(now() - thawed)}; the VIP was on two machines for at most ${secs(longestDouble)} as sampled`)
  if (unhealthy(watch.seen!) !== null) throw new Failed(`not healthy 120 s after the thaw: ${unhealthy(watch.seen!)}`)
  if (longestDouble > 5000) throw new Failed(`two VIP holders for ${secs(longestDouble)}`)
  if (leaderChanges > 0) throw new Failed(`the thawed ex-leader caused ${leaderChanges} leader change(s)`)
  note(`the leader stayed ${elected.value}; the ex-leader stepped down and released the VIP`)
})

await step('4', 'Reboot persistence: reset a CP voter', async (note) => {
  await healthy(5 * 60_000)
  const s0 = watch.seen!
  const victim = s0.cp.voters.find((v) => v !== s0.leader)!
  const uptime0 = s0.truth.vms.find((v) => v.id === victim)!.uptime_s ?? Infinity
  const voters0 = [...s0.cp.voters].sort().join()
  const k = await kube()
  const bootId0 = (await k.must(`/api/v1/nodes/${victim}`)).status.nodeInfo.bootID as string
  const resetAt = now()
  await fault({ Reset: { vm: victim } })
  let wentNotReady = false
  const rebooted = await waitFor(`${victim} rebooted (uptime reset)`, 120_000, (s) => {
    wentNotReady ||= s.hosts.get(victim)?.lifecycle === 'NotReady'
    const up = s.truth.vms.find((v) => v.id === victim)?.uptime_s
    // Only a sample taken after the reset, of a machine up no longer than that.
    return up !== null && up !== undefined && s.truth.at > resetAt && up < uptime0 && up <= (s.truth.at - resetAt) / 1000 + 1
  })
  note(`reset ${victim}; its guest agent reported a fresh boot ${secs(rebooted.ms)} later`)
  const back = await healthy(5 * 60_000)
  wentNotReady ||= false
  note(`back to healthy ${secs(back.ms)} after that${wentNotReady ? '' : ', never having gone NotReady (it rebooted inside the 40 s heartbeat timeout)'}`)
  const s = watch.seen!
  if ([...s.cp.voters].sort().join() !== voters0) throw new Failed(`voters changed: ${s.cp.voters}`)
  const h = s.hosts.get(victim)!
  if (h.net.state !== 'Applied') throw new Failed(`network ${h.net.state}`)
  note(`same voters; its network config is still generation ${h.net.applied_generation}, Applied (booted from the persisted copy)`)
  const rebooted8s = await waitAsync(`${victim} Ready in Kubernetes on its new boot`, 5 * 60_000, async () => {
    const n = await k.must(`/api/v1/nodes/${victim}`)
    return (n.status.nodeInfo.bootID !== bootId0 && isReady(n)) || `boot ${n.status.nodeInfo.bootID === bootId0 ? 'unchanged' : 'new'}, Ready ${isReady(n)}`
  })
  const k8s = await kubeHealthy(5 * 60_000)
  note(`K6: ${victim}'s kubelet reports its new boot and the node is Ready (${secs(rebooted8s.ms)}); every node Ready, the K3s servers still the voters`)
  void k8s
})

await step('K4', 'Volume and host failure: a pod\'s Longhorn volume survives losing its host, and its lost replica is rebuilt elsewhere', async (note) => {
  await healthy(5 * 60_000)
  await kubeHealthy(5 * 60_000)
  const k = await kube()
  const run = Math.random().toString(36).slice(2, 8)
  const token = `t-${run}-${Math.random().toString(36).slice(2, 10)}`
  const ns = 'e2e'
  await k.create('/api/v1/namespaces', { metadata: { name: ns } })
  // Replicas come back sooner than Longhorn's default 10 min wait for a failed one.
  await waitAsync('Longhorn to take replica-replenishment-wait-interval=20', 120_000, async () => {
    const r = await k.req('PATCH', `${LH}/settings/replica-replenishment-wait-interval`, { value: '20' }, 'application/merge-patch+json', 30_000)
    return r.status === 200 || `${r.status} ${r.text.slice(0, 200)}`
  })
  await k.create('/apis/storage.k8s.io/v1/storageclasses', {
    metadata: { name: 'e2e-2-replicas' },
    provisioner: 'driver.longhorn.io',
    reclaimPolicy: 'Delete',
    // best-effort locality keeps a replica on the pod's host, so losing that host loses one.
    parameters: { numberOfReplicas: '2', staleReplicaTimeout: '30', dataLocality: 'best-effort' },
  })
  const pvc = `data-${run}`
  await k.create(`/api/v1/namespaces/${ns}/persistentvolumeclaims`, {
    metadata: { name: pvc },
    spec: { accessModes: ['ReadWriteOnce'], storageClassName: 'e2e-2-replicas', resources: { requests: { storage: '1Gi' } } },
  })
  // The writer runs on a host Longhorn stores on (so a replica can live with it), not the homelabd leader.
  const storing: string[] = ((await k.must(`${LH}/nodes`)).items as Obj[])
    .filter((n) => Object.keys(n.spec.disks ?? {}).length > 0 && n.metadata.name !== watch.seen!.leader)
    .map((n) => n.metadata.name)
  if (storing.length < 3) throw new Failed(`only ${storing.length} storage hosts besides the leader: ${storing}`)
  const app = `writer-${run}`
  const script = 'if [ -f /data/token ]; then echo "found $(cat /data/token)"; else echo "$TOKEN" > /data/token && sync && echo "wrote $TOKEN"; fi; exec sleep 1000000'
  const toleration = (key: string) => ({ key, operator: 'Exists', effect: 'NoExecute', tolerationSeconds: 20 })
  await k.create(`/apis/apps/v1/namespaces/${ns}/deployments`, {
    metadata: { name: app },
    spec: {
      replicas: 1,
      strategy: { type: 'Recreate' },
      selector: { matchLabels: { app } },
      template: {
        metadata: { labels: { app } },
        spec: {
          terminationGracePeriodSeconds: 5,
          affinity: { nodeAffinity: { requiredDuringSchedulingIgnoredDuringExecution: { nodeSelectorTerms: [{ matchExpressions: [{ key: 'kubernetes.io/hostname', operator: 'In', values: storing }] }] } } },
          tolerations: [toleration('node.kubernetes.io/unreachable'), toleration('node.kubernetes.io/not-ready')],
          containers: [{ name: 'writer', image: 'busybox:1.37', command: ['sh', '-c', script], env: [{ name: 'TOKEN', value: token }], volumeMounts: [{ name: 'data', mountPath: '/data' }] }],
          volumes: [{ name: 'data', persistentVolumeClaim: { claimName: pvc } }],
        },
      },
    },
  })
  try {
    /** The deployment's running pod whose log says `want`, and its node. */
    const runningWith = (want: string, notOn?: string) => async () => {
      const pods: Obj[] = (await k.must(`/api/v1/namespaces/${ns}/pods?labelSelector=app%3D${app}`)).items
      const p = pods.find((p) => p.status.phase === 'Running' && !p.metadata.deletionTimestamp && p.spec.nodeName !== notOn)
      if (!p) return `pods: ${pods.map((p) => `${p.metadata.name}@${p.spec.nodeName}=${p.status.phase}${p.metadata.deletionTimestamp ? '(terminating)' : ''}`).join(', ') || 'none'}`
      const log = await k.get(`/api/v1/namespaces/${ns}/pods/${p.metadata.name}/log`)
      return log.text.includes(want) ? { pod: p.metadata.name as string, node: p.spec.nodeName as string, log: log.text.trim() } : `log of ${p.metadata.name}: ${log.text.slice(0, 80)}`
    }
    const first = await waitAsync('the writer running with its token written', 10 * 60_000, runningWith(`wrote ${token}`))
    const pv = (await k.must(`/api/v1/namespaces/${ns}/persistentvolumeclaims/${pvc}`)).spec.volumeName as string
    note(`${first.value.pod} on ${first.value.node} wrote its token to Longhorn volume ${pv} (${secs(first.ms)} to schedule, attach and start)`)
    /** The volume's healthy state: robustness, and its running replicas' nodes. */
    const volume = async () => {
      const v = await k.must(`${LH}/volumes/${pv}`)
      const reps: Obj[] = (await k.must(`${LH}/replicas?labelSelector=longhornvolume%3D${pv}`)).items
      const running = reps.filter((r) => r.status?.currentState === 'running' && !r.spec.failedAt).map((r) => r.spec.nodeID as string)
      return { robustness: v.status?.robustness as string, nodes: running, all: reps.map((r) => `${r.spec.nodeID}:${r.status?.currentState}${r.spec.failedAt ? '(failed)' : ''}`) }
    }
    const healthyVol = await waitAsync(`${pv} healthy with 2 replicas on 2 hosts, one on ${first.value.node}`, 5 * 60_000, async () => {
      const v = await volume()
      return (v.robustness === 'healthy' && new Set(v.nodes).size === 2 && v.nodes.includes(first.value.node) && v) || `${v.robustness} [${v.all}]`
    })
    note(`${pv} is healthy, replicas on ${healthyVol.value.nodes.join(' and ')}`)

    const victim = first.value.node
    const off = now()
    await fault({ PowerOff: { vm: victim } })
    note(`powered off ${victim}${victim === watch.seen!.leader ? ' (also the homelabd leader)' : ''}`)
    const notReady = await waitAsync(`${victim} NotReady in Kubernetes`, 3 * 60_000, async () => {
      const n = await k.get(`/api/v1/nodes/${victim}`)
      return (n.status === 200 && !isReady(n.body)) || 'still Ready'
    })
    note(`${victim} NotReady in Kubernetes after ${secs(now() - off)}`)
    void notReady
    const moved = await waitAsync('the writer running elsewhere, reading its token back', 10 * 60_000, runningWith(`found ${token}`, victim))
    note(`${moved.value.pod} runs on ${moved.value.node} and read back "${token}" ${secs(now() - off)} after the power-off`)
    const rebuilt = await waitAsync(`${pv} healthy again with 2 replicas, none on ${victim}`, 10 * 60_000, async () => {
      const v = await volume()
      return (v.robustness === 'healthy' && new Set(v.nodes).size === 2 && !v.nodes.includes(victim) && v) || `${v.robustness} [${v.all}]`
    })
    const fresh = rebuilt.value.nodes.filter((n) => !healthyVol.value.nodes.includes(n))
    if (fresh.length === 0) throw new Failed(`no replica was rebuilt: replicas on ${rebuilt.value.nodes} before and after`)
    note(`Longhorn rebuilt the lost replica onto ${fresh.join(', ')}: healthy on ${rebuilt.value.nodes.join(' and ')}, ${secs(now() - off)} after the power-off`)
    const hd = await waitFor(`${victim} NotReady in homelabd`, 120_000, (s) => s.hosts.get(victim)?.lifecycle === 'NotReady')
    void hd
    await fault({ PowerOn: { vm: victim } })
    const back = await healthy(5 * 60_000)
    const kb = await kubeHealthy(5 * 60_000)
    note(`powered ${victim} back on: Ready in homelabd after ${secs(back.ms)}, a Ready node again ${secs(kb.ms)} after that`)
  } finally {
    await k.delete(`/api/v1/namespaces/${ns}`)
    await k.delete('/apis/storage.k8s.io/v1/storageclasses/e2e-2-replicas')
  }
})

/** The switch ports `host`'s `nics` are cabled to, from the lab's ground truth. */
function portsOf(s: Seen, host: string, nics: string[]): PortTruth[] {
  return nics.map((n) => {
    const p = s.truth.ports.find((p) => p.host === host && p.nic === n)
    if (!p) throw new Failed(`${host} ${n} is not cabled`)
    return p
  })
}

/** The open alerts on `host`, of any kind. */
function openOn(s: Seen, host: string): Alert[] {
  return [...s.alerts.values()].filter((a) => a.resolved_at === null && a.entity.id === host)
}

/** An MS-01 voter that does not lead: losing a bond member must not cost the cluster its leader. */
function ms01Follower(s: Seen): string {
  const id = s.cp.voters.find((v) => v !== s.leader && v.startsWith('ms01'))
  if (!id) throw new Failed('no MS-01 follower')
  return id
}

/** Runs a control-plane operation; its `Ok` value, or the refusal. */
async function apiOp(op: unknown): Promise<unknown> {
  const res = await fetch(`${BASE}/api/ops`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(op) })
  if (!res.ok) throw new Failed(`POST /api/ops: ${res.status} ${await res.text()}`)
  const body = (await res.json()) as { Ok?: unknown; Err?: string } | string
  if (typeof body === 'object' && body !== null && 'Ok' in body) return body.Ok
  throw new Failed(`operation refused: ${JSON.stringify(body)}`)
}

/** All bytes the lab's switches took in on VLAN 20 (the storage network). */
function vlan20Rx(s: Seen): number {
  return s.truth.ports.reduce((n, p) => n + (p.counters.rx_vlan20_bytes ?? 0), 0)
}

await step('K8', 'Storage network: Longhorn replicates over VLAN 20 (Multus), and the traffic crosses it', async (note) => {
  await healthy(5 * 60_000)
  await kubeHealthy(5 * 60_000)
  const k = await kube()
  const setting = await k.must(`${LH}/settings/storage-network`)
  if (setting.value !== 'longhorn-system/storage-net') throw new Failed(`storage-network is ${JSON.stringify(setting.value)}`)
  note(`Longhorn setting storage-network is ${setting.value}`)
  const nad = await k.must('/apis/k8s.cni.cncf.io/v1/namespaces/longhorn-system/network-attachment-definitions/storage-net')
  const config = nad.spec?.config as string
  if (!config?.includes('"master": "storage0"') || !config.includes('"type": "macvlan"') || !config.includes('10.20.3.0/24')) {
    throw new Failed(`storage-net attachment is not a macvlan on storage0 with 10.20.3.0/24: ${(config ?? '').slice(0, 200)}`)
  }
  note('storage-net is a macvlan on storage0 (VLAN 20) with pod addresses from 10.20.3.0/24')
  const multus = await k.must('/apis/apps/v1/namespaces/kube-system/daemonsets/multus')
  if (!(multus.status?.numberReady > 0) || multus.status.numberReady !== multus.status.desiredNumberScheduled) {
    throw new Failed(`Multus not ready: ${multus.status?.numberReady}/${multus.status?.desiredNumberScheduled}`)
  }
  note(`Multus ready on ${multus.status.numberReady} nodes`)
  const ims: Obj[] = (await k.must('/api/v1/namespaces/longhorn-system/pods')).items.filter((p: Obj) => p.metadata.name.startsWith('instance-manager-'))
  const running = ims.filter((p) => p.status.phase === 'Running')
  if (running.length === 0) throw new Failed(`no running instance-managers (of ${ims.length})`)
  for (const p of running) {
    const status = p.metadata.annotations?.['k8s.v1.cni.cncf.io/network-status'] as string | undefined
    const nets: Obj[] = status ? JSON.parse(status) : []
    if (!nets.some((n) => n.name === 'longhorn-system/storage-net' && (n.ips as string[] | undefined)?.some((ip) => ip.startsWith('10.20.3.')))) {
      throw new Failed(`${p.metadata.name}@${p.spec.nodeName} has no storage-net interface in 10.20.3.0/24`)
    }
  }
  note(`${running.length} running instance-managers each carry a storage-net interface in 10.20.3.0/24`)

  // A 64 MiB write, replicated twice: the bytes must cross VLAN 20.
  // Its own namespace: K4 deletes `e2e` just before, and a terminating
  // namespace refuses new content.
  const run = Math.random().toString(36).slice(2, 8)
  const ns = 'e2e-k8'
  await k.create('/api/v1/namespaces', { metadata: { name: ns } })
  await k.create('/apis/storage.k8s.io/v1/storageclasses', {
    metadata: { name: 'e2e-k8' },
    provisioner: 'driver.longhorn.io',
    reclaimPolicy: 'Delete',
    parameters: { numberOfReplicas: '2', staleReplicaTimeout: '30', dataLocality: 'best-effort' },
  })
  const pvc = `k8-${run}`
  await k.create(`/api/v1/namespaces/${ns}/persistentvolumeclaims`, {
    metadata: { name: pvc },
    spec: { accessModes: ['ReadWriteOnce'], storageClassName: 'e2e-k8', resources: { requests: { storage: '1Gi' } } },
  })
  const storing: string[] = ((await k.must(`${LH}/nodes`)).items as Obj[])
    .filter((n) => Object.keys(n.spec.disks ?? {}).length > 0)
    .map((n) => n.metadata.name)
  if (storing.length < 2) throw new Failed(`only ${storing.length} storage hosts: ${storing}`)
  const app = `k8-writer-${run}`
  await k.create(`/apis/apps/v1/namespaces/${ns}/deployments`, {
    metadata: { name: app },
    spec: {
      replicas: 1,
      strategy: { type: 'Recreate' },
      selector: { matchLabels: { app } },
      template: {
        metadata: { labels: { app } },
        spec: {
          terminationGracePeriodSeconds: 5,
          affinity: { nodeAffinity: { requiredDuringSchedulingIgnoredDuringExecution: { nodeSelectorTerms: [{ matchExpressions: [{ key: 'kubernetes.io/hostname', operator: 'In', values: storing }] }] } } },
          containers: [
            {
              name: 'writer',
              image: 'busybox:1.37',
              command: ['sh', '-c', 'dd if=/dev/urandom of=/data/big bs=1M count=64 && sync && echo done && exec sleep 1000000'],
              volumeMounts: [{ name: 'data', mountPath: '/data' }],
            },
          ],
          volumes: [{ name: 'data', persistentVolumeClaim: { claimName: pvc } }],
        },
      },
    },
  })
  try {
    const before = vlan20Rx(watch.seen!)
    const written = await waitAsync('the 64 MiB write done', 10 * 60_000, async () => {
      const pods: Obj[] = (await k.must(`/api/v1/namespaces/${ns}/pods?labelSelector=app%3D${app}`)).items
      const p = pods.find((p) => p.status.phase === 'Running' && !p.metadata.deletionTimestamp)
      if (!p) return `pods: ${pods.map((p) => `${p.metadata.name}=${p.status.phase}`).join(', ') || 'none'}`
      const log = await k.get(`/api/v1/namespaces/${ns}/pods/${p.metadata.name}/log`)
      return log.text.includes('done') ? { node: p.spec.nodeName as string } : `log: ${log.text.slice(0, 80)}`
    })
    await sleep(5000)
    const delta = vlan20Rx(watch.seen!) - before
    note(`K8: ${written.value.node} wrote 64 MiB; ${(delta / 1048576).toFixed(1)} MiB crossed VLAN 20 meanwhile`)
    if (delta < 10 * 1048576) throw new Failed(`only ${(delta / 1048576).toFixed(1)} MiB crossed VLAN 20 during the replicated write`)
  } finally {
    await k.delete(`/apis/apps/v1/namespaces/${ns}/deployments/${app}`)
    await k.delete(`/api/v1/namespaces/${ns}/persistentvolumeclaims/${pvc}`)
    await k.delete('/apis/storage.k8s.io/v1/storageclasses/e2e-k8')
  }
})

await step('K7', 'Control-plane replacement: power off a K3s server, replace it, and etcd lists exactly the live servers', async (note) => {
  await healthy(5 * 60_000)
  await kubeHealthy(5 * 60_000)
  const s0 = watch.seen!
  const dead = ms01Follower(s0)
  // The replacement: Ready, not a voter, an MS-01 without held roles where one exists.
  const candidates = [...s0.hosts.values()]
    .filter((h) => h.lifecycle === 'Ready' && !s0.cp.voters.includes(h.id))
    .sort((a, b) => Number(b.id.startsWith('ms01')) - Number(a.id.startsWith('ms01')) || Number(Object.keys(b.net.held_roles).length === 0) - Number(Object.keys(a.net.held_roles).length === 0) || (a.id < b.id ? -1 : 1))
  const with_ = candidates[0]?.id
  if (!with_) throw new Failed('no Ready non-voter to replace with')
  const k = await kube()
  const off = now()
  await fault({ PowerOff: { vm: dead } })
  note(`powered off ${dead} (a K3s server and voter); ${with_} will take its place`)
  await waitFor(`${dead} NotReady in homelabd`, 120_000, (s) => s.hosts.get(dead)?.lifecycle === 'NotReady')
  await apiOp({ ReplaceCpMember: { dead, with: with_ } })
  const swapped = await waitFor(`voters without ${dead}, with ${with_}`, 5 * 60_000, (s) => s.cp.voters.length === 3 && !s.cp.voters.includes(dead) && s.cp.voters.includes(with_))
  note(`Raft swapped the voter ${secs(swapped.ms)} after the operation (voters [${[...watch.seen!.cp.voters].sort()}])`)
  const joined = await waitAsync(`${with_} a Ready K3s server and ${dead}'s node gone`, 10 * 60_000, async () => {
    const nodes: Obj[] = (await k.must('/api/v1/nodes')).items
    const w = nodes.find((n) => n.metadata.name === with_)
    const gone = !nodes.some((n) => n.metadata.name === dead)
    return w && isReady(w) && w.metadata.labels['node-role.kubernetes.io/control-plane'] === 'true' && gone && w
      ? { servers: nodes.filter((n) => n.metadata.labels['node-role.kubernetes.io/control-plane'] === 'true').map((n) => n.metadata.name).sort() }
      : `${with_}: ${w ? `Ready ${isReady(w)}, control-plane ${w.metadata.labels['node-role.kubernetes.io/control-plane']}` : 'absent'}; ${dead} ${gone ? 'gone' : 'still listed'}`
  })
  note(`${with_} joined as a server; ${dead}'s node is gone (${secs(joined.ms)}); K3s servers [${joined.value.servers}]`)
  const voters = [...watch.seen!.cp.voters].sort().join()
  if (joined.value.servers.join() !== voters) throw new Failed(`K3s servers [${joined.value.servers}] are not the voters [${voters}]`)
  const etcd = await waitAsync('etcd listing exactly the live servers', 5 * 60_000, async () => {
    const nodes: Obj[] = (await k.must('/api/v1/nodes')).items
    const labelled = nodes.filter((n) => n.metadata.labels['node-role.kubernetes.io/etcd'] === 'true').map((n) => n.metadata.name).sort()
    // K3s labels its etcd members; if it ever stops, the control-plane nodes are the same set.
    const members = labelled.length > 0 ? labelled : nodes.filter((n) => n.metadata.labels['node-role.kubernetes.io/control-plane'] === 'true').map((n) => n.metadata.name).sort()
    return (members.join() === voters && { members, via: labelled.length > 0 ? 'node-role.kubernetes.io/etcd' : 'node-role.kubernetes.io/control-plane (no etcd label set)' }) || `[${members}] via etcd label, voters [${voters}]`
  })
  note(`K7: etcd lists exactly the live servers [${etcd.value.members}] (via ${etcd.value.via}), ${secs(now() - off)} after the power-off`)
  await fault({ PowerOn: { vm: dead } })
  const back = await healthy(10 * 60_000)
  const kb = await kubeHealthy(10 * 60_000)
  const n = await k.must(`/api/v1/nodes/${dead}`)
  if (n.metadata.labels['node-role.kubernetes.io/control-plane'] === 'true') throw new Failed(`${dead} came back as a server`)
  note(`powered ${dead} back on: Ready in homelabd after ${secs(back.ms)}, a Ready agent node ${secs(kb.ms)} after that`)
})

await step('6a', 'Network: unplug one bond member; the bond degrades and the host keeps its other member', async (note) => {
  await healthy(5 * 60_000)
  const host = ms01Follower(watch.seen!)
  await waitFor(`no alerts open on ${host}`, 90_000, (s) => openOn(s, host).length === 0)
  const [, sfp1] = portsOf(watch.seen!, host, ['sfp0', 'sfp1'])
  await fault({ Unplug: { vm: host, nic: 'sfp1' } })
  try {
    const raised = await waitFor(`BondDegraded on ${host}`, 60_000, (s) => openAlerts(s, 'BondDegraded', host)[0])
    note(`unplugged ${host} sfp1 (${sfp1.switch} ${sfp1.port}): "${raised.value.message}" after ${secs(raised.ms)}`)
    const before = portsOf(watch.seen!, host, ['sfp0'])[0].counters.rx_frames
    await sleep(15_000)
    const s = watch.seen!
    if (s.hosts.get(host)?.lifecycle !== 'Ready') throw new Failed(`${host} is ${s.hosts.get(host)?.lifecycle}`)
    const [sfp0] = portsOf(s, host, ['sfp0'])
    if (!sfp0.link_up || sfp0.counters.rx_frames <= before) throw new Failed(`sfp0 carries nothing (link ${sfp0.link_up})`)
    // Ready says little here (management runs on eth0), and these frames are
    // LACP and LLDP; data over the bond (replica traffic on VLAN 20) is measured in K8.
    note(`${host} stays Ready; sfp0 (${sfp0.switch} ${sfp0.port}) is up and received ${sfp0.counters.rx_frames - before} frames in 15 s`)
  } finally {
    await fault({ Replug: { vm: host, nic: 'sfp1' } })
  }
  const cleared = await waitFor(`${host}'s alerts resolved`, 90_000, (s) => openOn(s, host).length === 0)
  note(`replugged: every alert on ${host} resolved after ${secs(cleared.ms)}`)
})

await step('6b', 'Network: remove VLAN 20 from a host\'s switch ports; a mismatch is flagged and storage stops', async (note) => {
  await healthy(5 * 60_000)
  const host = ms01Follower(watch.seen!)
  await waitFor(`no alerts open on ${host}`, 90_000, (s) => openOn(s, host).length === 0)
  const ports = portsOf(watch.seen!, host, ['sfp0', 'sfp1'])
  const cfgOf = (p: PortTruth) => {
    const cfg = watch.seen!.switches.get(p.switch)?.ports.find((o) => o.port === p.port)?.cfg
    if (!cfg || !('Trunk' in cfg.mode)) throw new Failed(`${p.switch} ${p.port} is not an observed trunk`)
    return cfg
  }
  const original = ports.map(cfgOf)
  const set = async (allowed: (cfg: SwitchPortCfg) => number[]) => {
    for (const [i, p] of ports.entries()) {
      const cfg = original[i]
      const mode = 'Trunk' in cfg.mode ? { Trunk: { ...cfg.mode.Trunk, allowed: allowed(cfg) } } : cfg.mode
      await fault({ SetSwitchPort: { switch: p.switch, port: p.port, cfg: { ...cfg, mode } } })
    }
  }
  const trunk = (cfg: SwitchPortCfg) => ('Trunk' in cfg.mode ? cfg.mode.Trunk.allowed : [])
  await set((cfg) => trunk(cfg).filter((v) => v !== 20))
  try {
    const raised = await waitFor(`a NetworkMismatch on ${host} naming VLAN 20, its storage role held`, 90_000, (s) => {
      const a = openAlerts(s, 'NetworkMismatch', host).find((a) => a.message.includes('VLAN 20'))
      return a && s.hosts.get(host)?.net.held_roles.Storage !== undefined && a
    })
    note(`took VLAN 20 off ${ports.map((p) => `${p.switch} ${p.port}`).join(' and ')}: "${raised.value.message}" after ${secs(raised.ms)} (switches are polled every 30 s)`)
    const held = watch.seen!.hosts.get(host)!.net.held_roles
    note(`roles held inactive: ${JSON.stringify(held)}`)
    const drops = () => portsOf(watch.seen!, host, ['sfp0', 'sfp1']).reduce((n, p) => n + p.counters.drop_vlan, 0)
    const before = drops()
    await sleep(10_000)
    note(`frames dropped at the switch for VLAN 20 not allowed, in 10 s: ${drops() - before} (replica traffic over VLAN 20 is measured in K8)`)
  } finally {
    await set(trunk)
  }
  const cleared = await waitFor(`the mismatch on ${host} resolved`, 90_000, (s) => openAlerts(s, 'NetworkMismatch', host).length === 0)
  note(`put VLAN 20 back: the mismatch resolved after ${secs(cleared.ms)}`)
})

watch.close()
console.log('\n==== results')
for (const r of results) console.log(`${r.ok ? 'PASS' : 'FAIL'}  [${r.id}] ${r.name}`)
process.exit(results.every((r) => r.ok) ? 0 : 1)
