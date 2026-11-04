// End-to-end validation of the cluster manager on the real VM lab.
//
// Talks only to the server's public interfaces: the WebSocket (the control
// plane's view and the lab's ground truth) and `POST /api/lab/fault` (things
// done to hardware). Each step is a success criterion of the real VM lab
// design (docs/plans/2026-10-26-real-vm-lab-design.md §10), timed, and the
// Kubernetes criteria K1–K8 of docs/plans/2026-10-27-k3s-longhorn-lab-design.md
// §5 (K9 drains a host; K10 applies, scales, restarts, edits and deletes a
// workload through `/api/kube`, as the Workloads tab does; K11 checks the
// health, usage, endpoints and events the tab shows; K12 logs, K13 network and
// volume usage), checked against the cluster's Kubernetes API through the VIP with the
// admin kubeconfig `GET /api/lab/kubeconfig` hands out (as an admin would
// get it with `sudo homelabd kubeconfig`).
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
import type { ApplyReport } from '../ui/src/gen/ApplyReport.ts'
import type { KubeAction } from '../ui/src/gen/KubeAction.ts'
import type { KubeView } from '../ui/src/gen/KubeView.ts'
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
  /** The server's read of Kubernetes, as the Workloads tab shows it. */
  kube: KubeView
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
        kube: m.kube,
      }
      return
    }
    const s = this.seen!
    s.leader = m.leader
    s.term = m.term
    s.truth = m.truth
    if (m.kube) s.kube = m.kube
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

/** The K3s member names `<node>-<8 hex digits>` as node names. */
const etcdNode = (name: string) => name.replace(/-[0-9a-f]{8}$/, '')

/**
 * etcd's members as etcd itself lists them, by node: a one-off pod on K3s
 * server `on` (host network) asks K3s's `/db/info` on loopback with the
 * server's admin certificate, which is how it answers. Independent of what
 * homelabd reports. A string says why not.
 */
async function etcdMembers(k: Kube, on: string): Promise<string[] | string> {
  const ns = 'e2e-etcd'
  await k.create('/api/v1/namespaces', { metadata: { name: ns } })
  const name = `db-info-${Math.random().toString(36).slice(2, 8)}`
  const tls = '/var/lib/rancher/k3s/server/tls'
  await k.create(`/api/v1/namespaces/${ns}/pods`, {
    metadata: { name },
    spec: {
      nodeName: on,
      hostNetwork: true,
      restartPolicy: 'Never',
      tolerations: [{ operator: 'Exists' }],
      containers: [
        {
          name: 'curl',
          image: 'curlimages/curl:8.23.0',
          securityContext: { runAsUser: 0 },
          command: ['curl', '-fsS', '--cacert', '/tls/server-ca.crt', '--cert', '/tls/client-admin.crt', '--key', '/tls/client-admin.key', 'https://127.0.0.1:6443/db/info'],
          volumeMounts: [{ name: 'tls', mountPath: '/tls', readOnly: true }],
        },
      ],
      volumes: [{ name: 'tls', hostPath: { path: tls } }],
    },
  })
  try {
    const done = await waitAsync(`the etcd query on ${on}`, 3 * 60_000, async () => {
      const p = await k.must(`/api/v1/namespaces/${ns}/pods/${name}`)
      return (['Succeeded', 'Failed'].includes(p.status.phase) && { phase: p.status.phase as string }) || `pod ${p.status.phase}`
    })
    const log = await k.get(`/api/v1/namespaces/${ns}/pods/${name}/log`)
    if (done.value.phase !== 'Succeeded') return `the etcd query failed: ${log.text.slice(0, 200)}`
    const members: Obj[] = JSON.parse(log.text).members
    return members.map((m) => (m.name ? etcdNode(m.name) : `(unstarted ${m.peerURLs})`)).sort()
  } finally {
    await k.delete(`/api/v1/namespaces/${ns}/pods/${name}`)
  }
}

/** An IPv4 address as a number, to compare against a range. */
const ipNum = (ip: string) => ip.split('.').reduce((n, o) => n * 256 + Number(o), 0)
/** The seed storage network's range for Longhorn's pods (`STORAGE_RESERVED`). */
const inStoragePods = (ip: string) => ipNum(ip) >= ipNum('10.20.3.0') && ipNum(ip) <= ipNum('10.20.3.255')

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

await step('3c', 'Move the VIP: hand the leadership to a chosen voter', async (note) => {
  await healthy(5 * 60_000)
  const k = await kube()
  const s0 = watch.seen!
  const [old, term] = [s0.leader!, s0.term]
  const to = s0.cp.voters.find((v) => v !== old)!
  const nonVoter = [...s0.hosts.keys()].find((h) => !s0.cp.voters.includes(h))
  const op = (body: unknown) => fetch(`${BASE}/api/ops`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) })
  if (nonVoter) {
    const refused = await op({ TransferCpLeadership: { to: nonVoter } })
    if (refused.status !== 400) throw new Failed(`moving the VIP to non-voter ${nonVoter}: ${refused.status} ${await refused.text()}`)
    note(`moving it to ${nonVoter}, no voter, is refused: ${(await refused.json()).error}`)
  }
  const asked = now()
  const res = await op({ TransferCpLeadership: { to } })
  if (!res.ok) throw new Failed(`TransferCpLeadership to ${to}: ${res.status} ${await res.text()}`)
  const led = await waitFor(`${to} leading`, 10_000, (s) => s.leader === to)
  note(`${old} handed the leadership to ${to}: the client saw it lead ${secs(now() - asked)} after asking, in term ${watch.seen!.term}`)
  if (watch.seen!.term !== term + 1) throw new Failed(`${to} leads in term ${watch.seen!.term}, not ${term + 1}: the hand-over took more than one election`)
  // An op sent in the tens of milliseconds while the VIP moves can be lost
  // (its outcome unknown, as with any change of leader); once the new leader
  // is seen, ops go straight to it.
  const t = now()
  const after = await op({ SetCpMode: { mode: s0.cp.mode } })
  if (!after.ok) throw new Failed(`an op to the new leader: ${after.status} ${await after.text()}`)
  if (now() - t > 2000) throw new Failed(`an op to the new leader took ${secs(now() - t)}`)
  note(`an op to the new leader was answered in ${secs(now() - t)}`)
  const vip = await waitFor(`the VIP on ${to} only`, 15_000, (s) => s.truth.vip_holders.length === 1 && s.truth.vip_holders[0] === to)
  note(`the VIP is on ${to} alone ${secs(led.ms + vip.ms)} after asking`)
  await waitAsync('the Kubernetes API through the VIP', 60_000, async () => {
    const r = await k.get('/readyz')
    return r.status === 200 || `readyz ${r.status} ${r.text.slice(0, 80)}`
  })
  note(`the Kubernetes API answers through the VIP (${to}'s K3s server) ${secs(now() - asked)} after asking`)
  await sleep(10_000)
  if (watch.seen!.leader !== to || unhealthy(watch.seen!) !== null) throw new Failed(`10 s on: leader ${watch.seen!.leader}, ${unhealthy(watch.seen!) ?? 'healthy'}`)
  note(`10 s on, ${to} still leads and every machine is healthy`)
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

await step('K7', 'Storage network: Longhorn replicates over VLAN 20, not the management network', async (note) => {
  await healthy(5 * 60_000)
  await kubeHealthy(5 * 60_000)
  const k = await kube()
  const setting = (await k.must(`${LH}/settings/storage-network`)).value as string
  if (setting !== 'longhorn-system/homelab-storage') throw new Failed(`Longhorn's storage-network setting is "${setting}"`)
  // Every instance manager has its storage network interface, with a pod address on VLAN 20.
  const ims: Obj[] = (await k.must('/api/v1/namespaces/longhorn-system/pods?labelSelector=longhorn.io%2Fcomponent%3Dinstance-manager')).items
  const lhnet = (p: Obj) => (JSON.parse(p.metadata.annotations?.['k8s.v1.cni.cncf.io/network-status'] ?? '[]') as Obj[]).find((n) => n.name === 'longhorn-system/homelab-storage')
  const missing = ims.filter((p) => !lhnet(p)?.ips?.every(inStoragePods))
  if (ims.length === 0 || missing.length) throw new Failed(`instance managers without a storage network address: ${missing.map((p) => p.spec.nodeName).join(', ') || 'none run'}`)
  note(`instance managers on the storage network: ${ims.map((p) => `${p.spec.nodeName} ${lhnet(p).interface} ${lhnet(p).ips}`).join(', ')}`)

  const run = Math.random().toString(36).slice(2, 8)
  const ns = 'e2e-net'
  await k.create('/api/v1/namespaces', { metadata: { name: ns } })
  await k.create('/apis/storage.k8s.io/v1/storageclasses', {
    metadata: { name: 'e2e-net-2-replicas' },
    provisioner: 'driver.longhorn.io',
    reclaimPolicy: 'Delete',
    // One replica with the writer, so the other one's copy crosses the network.
    parameters: { numberOfReplicas: '2', dataLocality: 'best-effort' },
  })
  const pvc = `net-${run}`
  await k.create(`/api/v1/namespaces/${ns}/persistentvolumeclaims`, {
    metadata: { name: pvc },
    spec: { accessModes: ['ReadWriteOnce'], storageClassName: 'e2e-net-2-replicas', resources: { requests: { storage: '2Gi' } } },
  })
  const s0 = watch.seen!
  const storing: string[] = ((await k.must(`${LH}/nodes`)).items as Obj[])
    .filter((n) => Object.keys(n.spec.disks ?? {}).length > 0 && n.metadata.name !== s0.leader && s0.hosts.get(n.metadata.name)?.net.held_roles.Storage === undefined)
    .map((n) => n.metadata.name)
    .sort()
  const writerHost = storing.find((h) => h.startsWith('ms01')) ?? storing[0]
  if (!writerHost) throw new Failed('no storage host besides the leader')
  // Rewrites 64 MiB, synced, every few seconds: Longhorn copies each write to the other replica.
  const script = 'i=0; while true; do dd if=/dev/urandom of=/data/blob bs=1M count=64 conv=fsync 2>/dev/null; i=$((i+1)); echo "wrote $i"; sleep 2; done'
  await k.create(`/api/v1/namespaces/${ns}/pods`, {
    metadata: { name: 'writer' },
    spec: {
      nodeName: writerHost,
      terminationGracePeriodSeconds: 2,
      containers: [{ name: 'writer', image: 'busybox:1.37', command: ['sh', '-c', script], volumeMounts: [{ name: 'data', mountPath: '/data' }] }],
      volumes: [{ name: 'data', persistentVolumeClaim: { claimName: pvc } }],
    },
  })
  try {
    const started = await waitAsync(`the writer on ${writerHost} writing`, 10 * 60_000, async () => {
      const log = await k.get(`/api/v1/namespaces/${ns}/pods/writer/log`)
      return log.text.includes('wrote 1') || `log: ${log.text.slice(0, 80)}`
    })
    const pv = (await k.must(`/api/v1/namespaces/${ns}/persistentvolumeclaims/${pvc}`)).spec.volumeName as string
    note(`the writer on ${writerHost} rewrites 64 MiB at a time on Longhorn volume ${pv} (${secs(started.ms)} to start)`)
    const healthyVol = await waitAsync(`${pv} healthy with 2 replicas, one on ${writerHost}`, 5 * 60_000, async () => {
      const v = await k.must(`${LH}/volumes/${pv}`)
      const reps: Obj[] = (await k.must(`${LH}/replicas?labelSelector=longhornvolume%3D${pv}`)).items
      const nodes = reps.filter((r) => r.status?.currentState === 'running' && !r.spec.failedAt).map((r) => r.spec.nodeID as string)
      return (v.status?.robustness === 'healthy' && new Set(nodes).size === 2 && nodes.includes(writerHost) && nodes) || `${v.status?.robustness} [${nodes}]`
    })
    const remote = healthyVol.value.find((n) => n !== writerHost)!
    // The engine reaches its replicas at their storage network addresses.
    const engines: Obj[] = (await k.must(`${LH}/engines?labelSelector=longhornvolume%3D${pv}`)).items
    const addrs = Object.values(engines[0]?.spec?.replicaAddressMap ?? {}) as string[]
    if (addrs.length !== 2 || !addrs.every((a) => inStoragePods(a.split(':')[0]))) throw new Failed(`the engine reaches its replicas at ${addrs.join(', ') || 'nothing'}, not on the storage network`)
    note(`replicas on ${writerHost} and ${remote}; the engine reaches them at ${addrs.join(' and ')} (VLAN 20 pod addresses)`)

    // What the writer's host sends into the switches, by VLAN, while it writes.
    const sent = () => {
      const by = new Map<number, number>()
      for (const p of watch.seen!.truth.ports.filter((p) => p.host === writerHost)) {
        for (const [vlan, bytes] of Object.entries(p.vlan_rx_bytes)) by.set(Number(vlan), (by.get(Number(vlan)) ?? 0) + bytes)
      }
      return by
    }
    const before = sent()
    await sleep(30_000)
    const after = sent()
    const delta = (vlan: number) => (after.get(vlan) ?? 0) - (before.get(vlan) ?? 0)
    const mib = (b: number) => `${(b / 1048576).toFixed(1)} MiB`
    note(`in 30 s ${writerHost} sent ${mib(delta(20))} on VLAN 20 (storage) and ${mib(delta(1))} on VLAN 1 (management)`)
    if (delta(20) < 64 * 1048576) throw new Failed(`only ${mib(delta(20))} crossed VLAN 20 while the writer rewrote 64 MiB blocks`)
    if (delta(1) * 4 > delta(20)) throw new Failed(`the management network carried ${mib(delta(1))}, against ${mib(delta(20))} on VLAN 20`)
  } finally {
    await k.delete(`/api/v1/namespaces/${ns}`)
    await k.delete('/apis/storage.k8s.io/v1/storageclasses/e2e-net-2-replicas')
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
    // LACP and LLDP; data over the bond is measured once workloads run (phase E).
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
    note(`frames dropped at the switch for VLAN 20 not allowed, in 10 s: ${drops() - before} (storage traffic itself is measured once workloads run, phase E)`)
  } finally {
    await set(trunk)
  }
  const cleared = await waitFor(`the mismatch on ${host} resolved`, 90_000, (s) => openAlerts(s, 'NetworkMismatch', host).length === 0)
  note(`put VLAN 20 back: the mismatch resolved after ${secs(cleared.ms)}`)
})

await step('K8', 'Replace a K3s server: power one off, replace it; etcd lists exactly the live servers', async (note) => {
  await healthy(5 * 60_000)
  await kubeHealthy(5 * 60_000)
  const k = await kube()
  const s0 = watch.seen!
  const voters = () => [...watch.seen!.cp.voters].sort()
  const before = await etcdMembers(k, s0.leader!)
  if (typeof before === 'string' || before.join() !== voters().join()) throw new Failed(`etcd [${before}] is not the voters [${voters()}] to start with`)
  const victim = s0.cp.voters.find((v) => v !== s0.leader)!
  const ready = (h: HostObj) => h.lifecycle === 'Ready' && h.profile.id === 'Ms01' && Object.keys(h.net.held_roles).length === 0
  const spare = [...s0.hosts.values()].filter((h) => ready(h) && !s0.cp.voters.includes(h.id)).map((h) => h.id).sort()[0]
  if (!spare) throw new Failed('no spare MS-01 to replace a server with')
  note(`etcd lists [${before}], the voters; powering off ${victim}, a K3s server`)
  await fault({ PowerOff: { vm: victim } })
  const off = now()
  await waitFor(`${victim} NotReady`, 90_000, (s) => s.hosts.get(victim)?.lifecycle === 'NotReady')
  // A dead voter is replaced, not handed off (the UI offers only Replace then).
  const handoff = await fetch(`${BASE}/api/ops`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ HandOffCp: { from: s0.leader, to: spare } }) })
  const refusal = await handoff.text()
  if (handoff.status !== 400 || !refusal.includes('replace it first')) throw new Failed(`a hand-off while ${victim} is down: ${handoff.status} ${refusal}`)
  note(`a hand-off while ${victim} is down is refused: ${JSON.parse(refusal).error}`)
  // As the CpReplacementSuggested alert and the UI's Replace button offer it; asked for at once here.
  const res = await fetch(`${BASE}/api/ops`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ ReplaceCpMember: { dead: victim, with: spare } }) })
  if (!res.ok) throw new Failed(`ReplaceCpMember: ${res.status} ${await res.text()}`)
  const asked = now()
  note(`${victim} NotReady ${secs(asked - off)} after the power-off; replacing it with ${spare}`)
  const swapped = await waitFor(`${spare} a voter in place of ${victim}`, 5 * 60_000, (s) => s.cp.voters.includes(spare) && !s.cp.voters.includes(victim))
  note(`the voters are ${voters()} ${secs(swapped.ms)} later`)
  const server = await waitAsync(`${spare} a Ready K3s server`, 10 * 60_000, async () => {
    const n = await k.get(`/api/v1/nodes/${spare}`)
    const h = watch.seen!.hosts.get(spare)
    const isServer = n.status === 200 && n.body.metadata.labels['node-role.kubernetes.io/control-plane'] === 'true' && isReady(n.body)
    return (isServer && h?.kube?.role === 'Server' && h.kube.phase === 'Running') || `node ${n.status === 200 ? (isReady(n.body) ? 'Ready' : 'not Ready') : n.status}, homelabd: ${h?.kube?.role} ${JSON.stringify(h?.kube?.phase)} ${h?.kube?.detail ?? ''}`
  })
  note(`${spare} runs a K3s server, Ready, ${secs(now() - asked)} after the replacement was asked for`)
  const live = await waitAsync('etcd to list exactly the live servers', 5 * 60_000, async () => {
    const m = await etcdMembers(k, watch.seen!.leader!)
    return (typeof m !== 'string' && m.join() === voters().join() && m) || `etcd [${m}], voters [${voters()}]`
  })
  const v = watch.seen!.truth.vms.find((v) => v.id === victim)
  if (v?.power !== 'Off') throw new Failed(`${victim} is ${v?.power}: the check needs it still off`)
  note(`etcd lists [${live.value}], exactly the live servers, while ${victim} is still off; ${victim}'s node is ${(await k.get(`/api/v1/nodes/${victim}`)).status === 404 ? 'gone' : 'still there'}`)
  const nodes: Obj[] = (await k.must('/api/v1/nodes')).items
  const servers = nodes.filter((n) => n.metadata.labels['node-role.kubernetes.io/control-plane'] === 'true').map((n) => n.metadata.name).sort()
  if (servers.join() !== voters().join()) throw new Failed(`K3s servers [${servers}] are not the voters [${voters()}]`)
  void server

  await fault({ PowerOn: { vm: victim } })
  const back = await healthy(10 * 60_000)
  const kb = await kubeHealthy(15 * 60_000)
  const after = await etcdMembers(k, watch.seen!.leader!)
  if (typeof after === 'string' || after.join() !== voters().join()) throw new Failed(`after ${victim} came back, etcd lists [${after}], voters [${voters()}]`)
  const h = watch.seen!.hosts.get(victim)
  note(`${victim} powered on: Ready ${secs(back.ms)} later, a K3s ${h?.kube?.role === 'Agent' ? 'agent' : h?.kube?.role} and every node Ready ${secs(kb.ms)} after that; etcd still lists [${after}]`)
})

await step('K9', 'Drain: a drained host\'s node is cordoned and its pods move off; uncordoning gives the node back', async (note) => {
  await healthy(5 * 60_000)
  await kubeHealthy(5 * 60_000)
  const k = await kube()
  const ns = 'e2e-drain'
  const app = `web-${Math.random().toString(36).slice(2, 8)}`
  await k.create('/api/v1/namespaces', { metadata: { name: ns } })
  const op = async (body: unknown) => {
    const res = await fetch(`${BASE}/api/ops`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) })
    if (!res.ok) throw new Failed(`${JSON.stringify(body)}: ${res.status} ${await res.text()}`)
  }
  let victim: string | null = null
  try {
    await k.create(`/apis/apps/v1/namespaces/${ns}/deployments`, {
      metadata: { name: app },
      spec: {
        replicas: 2,
        selector: { matchLabels: { app } },
        template: {
          metadata: { labels: { app } },
          spec: { terminationGracePeriodSeconds: 5, containers: [{ name: 'web', image: 'busybox:1.37', command: ['sh', '-c', 'exec sleep 1000000'] }] },
        },
      },
    })
    const pods = async () => ((await k.must(`/api/v1/namespaces/${ns}/pods?labelSelector=app%3D${app}`)).items as Obj[]).filter((p) => !p.metadata.deletionTimestamp)
    const placed = await waitAsync(`${app}'s 2 pods running`, 5 * 60_000, async () => {
      const ps = await pods()
      const running = ps.filter((p) => p.status.phase === 'Running')
      return (running.length === 2 && running.map((p) => p.spec.nodeName as string)) || `${ps.map((p) => `${p.metadata.name}@${p.spec.nodeName}=${p.status.phase}`)}`
    })
    // Not the homelabd leader: draining it would also move the VIP's K3s server work around.
    victim = placed.value.find((n) => n !== watch.seen!.leader) ?? placed.value[0]
    note(`${app}'s pods run on ${placed.value.join(' and ')}; draining ${victim}`)
    const asked = now()
    await op({ Drain: { host: victim } })
    const drained = await waitFor(`${victim} Drained`, 5 * 60_000, (s) => s.hosts.get(victim!)?.lifecycle === 'Drained')
    const node = await k.must(`/api/v1/nodes/${victim}`)
    if (node.spec.unschedulable !== true) throw new Failed(`${victim} is Drained but its node is schedulable`)
    const left = (await pods()).filter((p) => p.spec.nodeName === victim)
    if (left.length) throw new Failed(`${victim} is Drained but ${left.map((p) => p.metadata.name)} still run there`)
    const moved = await waitAsync(`${app}'s 2 pods running off ${victim}`, 5 * 60_000, async () => {
      const ps = (await pods()).filter((p) => p.status.phase === 'Running')
      return (ps.length === 2 && ps.every((p) => p.spec.nodeName !== victim) && ps.map((p) => p.spec.nodeName as string)) || `${ps.map((p) => `${p.metadata.name}@${p.spec.nodeName}`)}`
    })
    note(`${victim} Drained ${secs(drained.ms)} after the drain was asked for, its node cordoned; the pods run on ${moved.value.join(' and ')} (${secs(now() - asked)})`)
    await op({ Uncordon: { id: victim } })
    const back = await waitAsync(`${victim}'s node schedulable again`, 2 * 60_000, async () => {
      const n = await k.must(`/api/v1/nodes/${victim}`)
      return n.spec.unschedulable !== true || 'still cordoned'
    })
    victim = null
    note(`uncordoned: the node is schedulable ${secs(back.ms)} later`)
  } finally {
    if (victim) await op({ Uncordon: { id: victim } }).catch(() => {})
    await k.delete(`/api/v1/namespaces/${ns}`)
  }
})

await step('K10', 'Workload changes: the server applies YAML (preview first), scales, restarts, edits and deletes a workload as kubectl would, and refuses the system namespaces', async (note) => {
  await kubeHealthy(5 * 60_000)
  const k = await kube()
  const ns = 'e2e-actions'
  const app = `web-${Math.random().toString(36).slice(2, 8)}`
  const workload = { namespace: ns, kind: 'Deployment' as const, name: app }
  /** `POST /api/kube`: the status and the server's error, if any. */
  const act = async (a: KubeAction) => {
    const res = await fetch(`${BASE}/api/kube`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(a) })
    return { status: res.status, error: res.ok ? null : ((await res.json().catch(() => ({}))) as { error?: string }).error ?? res.statusText }
  }
  const must = async (a: KubeAction) => {
    const r = await act(a)
    if (r.status !== 200) throw new Failed(`${JSON.stringify(a)}: ${r.status} ${r.error}`)
  }
  /** `POST /api/kube/apply` that must answer 200: each object's outcome, as `<kind>/<name>=<outcome>`. */
  const apply = async (yaml: string, dry_run: boolean, force = false) => {
    const res = await fetch(`${BASE}/api/kube/apply`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ yaml, namespace: ns, dry_run, force }) })
    if (!res.ok) throw new Failed(`apply: ${res.status} ${await res.text()}`)
    const report = (await res.json()) as ApplyReport
    return report.objects.map((o) => `${o.kind}/${o.name}=${typeof o.outcome === 'string' ? o.outcome : Object.keys(o.outcome)[0]}`).join(' ')
  }
  const expect = (what: string, got: string, want: string) => {
    if (got !== want) throw new Failed(`${what}: ${got}, not ${want}`)
  }
  // The Deployment first and no namespace in it: the server puts the Namespace first and the Deployment in `ns`.
  const manifests = `apiVersion: apps/v1
kind: Deployment
metadata: { name: ${app} }
spec:
  replicas: 1
  selector: { matchLabels: { app: ${app} } }
  template:
    metadata: { labels: { app: ${app} } }
    spec:
      terminationGracePeriodSeconds: 5
      containers: [{ name: web, image: "busybox:1.37", command: [sh, -c, "exec sleep 1000000"] }]
---
apiVersion: v1
kind: Namespace
metadata: { name: ${ns} }
`
  try {
    expect('previewing', await apply(manifests, true), `Namespace/${ns}=Created Deployment/${app}=AfterNamespace`)
    if ((await k.get(`/api/v1/namespaces/${ns}`)).status !== 404) throw new Failed(`the preview created ${ns}`)
    expect('applying', await apply(manifests, false), `Namespace/${ns}=Created Deployment/${app}=Created`)
    expect('previewing again', await apply(manifests, true), `Namespace/${ns}=Unchanged Deployment/${app}=Unchanged`)
    note(`applied YAML: the preview said what would be created and created nothing; then ${ns} and ${app} were created, and a second preview finds them unchanged`)
    const running = async () =>
      ((await k.must(`/api/v1/namespaces/${ns}/pods?labelSelector=app%3D${app}`)).items as Obj[]).filter((p) => !p.metadata.deletionTimestamp && p.status.phase === 'Running').map((p) => p.metadata.name as string)
    const pods = async (n: number) => {
      const ps = await running()
      return (ps.length === n && ps) || `${ps.length} running: ${ps}`
    }
    await waitAsync(`${app}'s pod running`, 5 * 60_000, () => pods(1))

    let asked = now()
    await must({ Scale: { workload, replicas: 2 } })
    const scaled = await waitAsync(`${app} scaled to 2 running pods`, 3 * 60_000, () => pods(2))
    note(`scaled to 2: both pods running ${secs(now() - asked)} after asking`)

    asked = now()
    await must({ Restart: { workload } })
    const restarted = await waitAsync(`${app}'s pods replaced`, 3 * 60_000, async () => {
      const ps = await running()
      return (ps.length === 2 && ps.every((p) => !scaled.value.includes(p)) && ps) || `running: ${ps}; before: ${scaled.value}`
    })
    const annotations = (await k.must(`/apis/apps/v1/namespaces/${ns}/deployments/${app}`)).spec.template.metadata.annotations ?? {}
    if (!annotations['kubectl.kubernetes.io/restartedAt']) throw new Failed('restarted, but the pod template has no restartedAt annotation')
    note(`restarted: 2 new pods (${restarted.value.join(', ')}) running ${secs(now() - asked)} after asking`)

    const refused = await act({ Restart: { workload: { namespace: 'kube-system', kind: 'Deployment', name: 'coredns' } } })
    if (refused.status !== 400) throw new Failed(`restarting kube-system/coredns: ${refused.status}, not refused`)
    const daemon = await act({ Scale: { workload: { ...workload, kind: 'DaemonSet' }, replicas: 3 } })
    if (daemon.status !== 400) throw new Failed(`scaling a DaemonSet: ${daemon.status}, not refused`)
    const missing = await act({ Restart: { workload: { ...workload, name: `${app}-none` } } })
    if (missing.status !== 404) throw new Failed(`restarting a workload that does not exist: ${missing.status} ${missing.error}`)
    note(`refused: kube-system (${refused.error}); scaling a DaemonSet; a missing workload is Kubernetes' 404 (${missing.error})`)

    // Edit YAML: the live Deployment as a manifest, changed and applied again.
    const res = await fetch(`${BASE}/api/kube/manifest?namespace=${ns}&kind=Deployment&name=${app}`)
    const live = await res.text()
    if (!res.ok || !live.includes('replicas: 2') || live.includes('resourceVersion') || live.includes('status:')) throw new Failed(`the manifest to edit: ${res.status} ${live.slice(0, 300)}`)
    // The UI's Scale set replicas through the scale subresource: an edit takes it over, as the Edit YAML dialog does.
    const edited = live.replace('replicas: 2', 'replicas: 1')
    expect('previewing the edit without taking over', await apply(edited, true), `Deployment/${app}=Conflict`)
    expect('previewing the edit', await apply(edited, true, true), `Deployment/${app}=Configured`)
    expect('applying the edit', await apply(edited, false, true), `Deployment/${app}=Configured`)
    await waitAsync(`${app} back to 1 running pod`, 3 * 60_000, () => pods(1))
    note('edited: the live manifest came without status or bookkeeping, and applying it with 1 replica scaled the Deployment down')

    // A field another manager set: a conflict, until taken over.
    await k.create(`/api/v1/namespaces/${ns}/configmaps`, { metadata: { name: 'settings' }, data: { mode: 'a' } })
    const settings = 'apiVersion: v1\nkind: ConfigMap\nmetadata: { name: settings }\ndata: { mode: b }\n'
    expect('previewing a change to a field another manager owns', await apply(settings, true), 'ConfigMap/settings=Conflict')
    expect('taking it over', await apply(settings, false, true), 'ConfigMap/settings=Configured')
    const mode = (await k.must(`/api/v1/namespaces/${ns}/configmaps/settings`)).data.mode
    if (mode !== 'b') throw new Failed(`taken over, but mode is ${mode}`)
    const intoSystem = await fetch(`${BASE}/api/kube/apply`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ yaml: settings, namespace: 'kube-system', dry_run: true, force: false }) })
    if (intoSystem.status !== 400) throw new Failed(`applying into kube-system: ${intoSystem.status}, not refused`)
    note('a field another manager set conflicts until taken over; applying into kube-system is refused')

    asked = now()
    await must({ Delete: { workload } })
    await waitAsync(`${app} and its pods gone`, 3 * 60_000, async () => {
      const d = await k.get(`/apis/apps/v1/namespaces/${ns}/deployments/${app}`)
      const left = await k.must(`/api/v1/namespaces/${ns}/pods?labelSelector=app%3D${app}`)
      return (d.status === 404 && left.items.length === 0) || `deployment ${d.status}, ${left.items.length} pods left`
    })
    note(`deleted: the Deployment and its pods gone ${secs(now() - asked)} after asking`)
  } finally {
    await k.delete(`/api/v1/namespaces/${ns}`)
  }
})

await step('K11', 'Workload details: a healthy workload shows its usage, limits and an Ingress endpoint that answers; a broken one shows down, why, and its Warning events', async (note) => {
  await kubeHealthy(5 * 60_000)
  const k = await kube()
  const ns = 'e2e-details'
  const tag = Math.random().toString(36).slice(2, 8)
  const path = `/k11-${tag}`
  const manifests = `apiVersion: v1
kind: Namespace
metadata: { name: ${ns} }
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: site }
spec:
  replicas: 2
  selector: { matchLabels: { app: site } }
  template:
    metadata: { labels: { app: site } }
    spec:
      terminationGracePeriodSeconds: 2
      containers:
        - name: site
          image: "busybox:1.37"
          command: [sh, -c, "mkdir -p /www${path} && echo k11-${tag}-ok > /www${path}/index.html && exec httpd -f -p 8080 -h /www"]
          ports: [{ containerPort: 8080 }]
          resources: { limits: { cpu: 200m, memory: 64Mi } }
---
apiVersion: v1
kind: Service
metadata: { name: site }
spec:
  selector: { app: site }
  ports: [{ port: 80, targetPort: 8080 }]
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: site }
spec:
  rules:
    - http:
        paths: [{ path: "${path}", pathType: Prefix, backend: { service: { name: site, port: { number: 80 } } } }]
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: broken }
spec:
  replicas: 1
  selector: { matchLabels: { app: broken } }
  template:
    metadata: { labels: { app: broken } }
    spec:
      containers: [{ name: broken, image: "registry.invalid/none:1" }]
`
  const res = await fetch(`${BASE}/api/kube/apply`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ yaml: manifests, namespace: ns, dry_run: false, force: false }) })
  if (!res.ok) throw new Failed(`apply: ${res.status} ${await res.text()}`)
  try {
    const workload = (name: string) => watch.seen!.kube.workloads.find((w) => w.namespace === ns && w.name === name)
    const site = await waitFor('site healthy with its usage', 5 * 60_000, () => {
      const w = workload('site')
      return w?.health.state === 'Healthy' && w.memory_bytes !== null && w.cpu_milli !== null ? w : null
    })
    const w = site.value
    if (w.cpu_limit_milli !== 400 || w.memory_limit_bytes !== 128 * 2 ** 20) throw new Failed(`site's limits: ${w.cpu_limit_milli}m and ${w.memory_limit_bytes} bytes, not 400m and 128Mi (2 pods)`)
    const ingress = w.endpoints.find((e) => e.kind === 'Ingress')
    if (!ingress?.link?.endsWith(path)) throw new Failed(`site's endpoints: ${JSON.stringify(w.endpoints)}`)
    if (!w.endpoints.some((e) => e.address === `site.${ns}:80`)) throw new Failed(`no in-cluster endpoint: ${JSON.stringify(w.endpoints)}`)
    const page = await waitAsync(`${ingress.link} answering`, 2 * 60_000, async () => {
      const r = await fetch(`${ingress.link}/`)
      const body = await r.text()
      return (r.ok && body.includes(`k11-${tag}-ok`) && { body: body.trim() }) || `${r.status} ${body.slice(0, 80)}`
    })
    note(`site: healthy ${secs(site.ms)} after applying, using ${w.cpu_milli}m and ${Math.round(w.memory_bytes! / 2 ** 20)}Mi of 400m and 128Mi; ${ingress.address} answers "${page.value.body}"`)

    const broken = await waitFor('broken down, for its image', 3 * 60_000, () => {
      const b = workload('broken')
      return b?.health.state === 'Down' && /ImagePullBackOff|ErrImagePull/.test(b.health.reason ?? '') ? b : null
    })
    const events = await waitFor("broken's Warning events", 2 * 60_000, () => {
      const es = watch.seen!.kube.events.filter((e) => e.namespace === ns && e.workload === 'Deployment/broken')
      return es.length > 0 ? es : null
    })
    note(`broken: down (${broken.value.health.reason}); events: ${[...new Set(events.value.map((e) => e.reason))].join(', ')}`)
  } finally {
    await k.delete(`/api/v1/namespaces/${ns}`)
  }
})

/** Waits until namespace `ns` is gone, so a step starts clean even right after its last run (deleting one takes a while). */
async function namespaceGone(ns: string) {
  const k = await kube()
  await waitAsync(`namespace ${ns} gone`, 3 * 60_000, async () => {
    const r = await k.get(`/api/v1/namespaces/${ns}`)
    return r.status === 404 || `${r.status} ${r.body?.status?.phase ?? ''}`
  })
}

/** Applies `yaml` through the server, as the Apply YAML dialog does. */
async function apply(yaml: string, namespace: string) {
  const res = await fetch(`${BASE}/api/kube/apply`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ yaml, namespace, dry_run: false, force: false }) })
  if (!res.ok) throw new Failed(`apply: ${res.status} ${await res.text()}`)
}

/** Reads `GET /api/kube/logs?<query>` for at most `ms` (or to its end): its status, and its lines without their timestamps. */
async function readLog(query: Record<string, string>, ms: number): Promise<{ status: number; lines: string[]; body: string }> {
  const abort = new AbortController()
  const timer = setTimeout(() => abort.abort(), ms)
  let body = ''
  let status = 0
  try {
    const res = await fetch(`${BASE}/api/kube/logs?${new URLSearchParams(query)}`, { signal: abort.signal })
    status = res.status
    const reader = res.body!.getReader()
    const decoder = new TextDecoder()
    for (;;) {
      const { done, value } = await reader.read()
      if (done) break
      body += decoder.decode(value, { stream: true })
    }
  } catch (e) {
    if (!abort.signal.aborted) throw e
  } finally {
    clearTimeout(timer)
  }
  const lines = body.split('\n').filter(Boolean).map((l) => l.replace(/^\S+Z /, ''))
  return { status, lines, body }
}

await step('K12', 'Logs: the server streams a container\'s log, follows it, gives the run before a crash, and refuses names that are not names', async (note) => {
  await kubeHealthy(5 * 60_000)
  const k = await kube()
  const ns = 'e2e-logs'
  await namespaceGone(ns)
  // Counts to 8 a second apart, then fails: Kubernetes restarts it, and the run before is the previous log.
  await apply(
    `apiVersion: v1
kind: Namespace
metadata: { name: ${ns} }
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: counter }
spec:
  replicas: 1
  selector: { matchLabels: { app: counter } }
  template:
    metadata: { labels: { app: counter } }
    spec:
      terminationGracePeriodSeconds: 1
      containers:
        - name: counter
          image: "busybox:1.37"
          command: [sh, -c, "i=0; while [ $i -lt 8 ]; do i=$((i+1)); echo line $i; sleep 1; done; exit 3"]
`,
    ns,
  )
  try {
    const pod = await waitFor('the counter pod running', 3 * 60_000, () => watch.seen!.kube.pods.find((p) => p.namespace === ns && p.status === 'Running'))
    const name = pod.value.name
    await sleep(3000)
    const tail = await readLog({ namespace: ns, pod: name, tail: '2', follow: 'false' }, 10_000)
    if (tail.status !== 200 || tail.lines.length !== 2 || !tail.lines.every((l) => /^line \d$/.test(l))) throw new Failed(`tail=2: ${tail.status} ${JSON.stringify(tail.lines)}`)
    const followed = await readLog({ namespace: ns, pod: name, tail: '1' }, 4000)
    const numbers = followed.lines.map((l) => Number(l.split(' ')[1]))
    if (numbers.length < 2 || numbers.some((n, i) => i > 0 && n !== numbers[i - 1] + 1)) throw new Failed(`following gave ${JSON.stringify(followed.lines)}`)
    note(`tail=2 gave "${tail.lines.join('", "')}"; following for 4 s gave ${numbers.length} lines in order (${numbers[0]}…${numbers.at(-1)})`)

    const restarted = await waitFor('the counter restarted', 2 * 60_000, () => watch.seen!.kube.pods.find((p) => p.name === name && p.restarts > 0))
    const previous = await readLog({ namespace: ns, pod: name, previous: 'true', follow: 'false', tail: '0' }, 10_000)
    if (previous.lines.at(-1) !== 'line 8') throw new Failed(`previous run's log ends with ${JSON.stringify(previous.lines.slice(-2))}`)
    note(`after a restart ${secs(restarted.ms)} later (exit ${restarted.value.last_termination?.exit_code}), previous=true ends with "line 8"`)

    const bad = await readLog({ namespace: ns, pod: '../secrets' }, 5000)
    const missing = await readLog({ namespace: ns, pod: 'nope', follow: 'false' }, 5000)
    if (bad.status !== 400 || missing.status !== 404) throw new Failed(`bad name: ${bad.status}, missing pod: ${missing.status} ${missing.body}`)
    const system = watch.seen!.kube.pods.find((p) => p.namespace === 'kube-system' && p.status === 'Running')!
    const sys = await readLog({ namespace: 'kube-system', pod: system.name, tail: '1', follow: 'false' }, 10_000)
    if (sys.status !== 200) throw new Failed(`a kube-system pod's log: ${sys.status} ${sys.body}`)
    note(`a bad name is refused (400), a missing pod is Kubernetes' 404 (${JSON.parse(missing.body).error}), and ${system.name}'s log reads (logs are reads)`)

  } finally {
    await k.delete(`/api/v1/namespaces/${ns}`)
  }
})

await step('K13', 'Network and disk: a workload serving a file through its Ingress shows its send rate; writing to a Longhorn claim shows in its used bytes', async (note) => {
  await kubeHealthy(5 * 60_000)
  const k = await kube()
  const ns = 'e2e-usage'
  await namespaceGone(ns)
  // Prefix, not Exact: Traefik ranks rules by length, and the demo's PathPrefix(`/`) outranks Path(`/blob`).
  await apply(
    `apiVersion: v1
kind: Namespace
metadata: { name: ${ns} }
---
apiVersion: apps/v1
kind: StatefulSet
metadata: { name: store }
spec:
  replicas: 1
  serviceName: store
  selector: { matchLabels: { app: store } }
  template:
    metadata: { labels: { app: store } }
    spec:
      terminationGracePeriodSeconds: 1
      nodeSelector: { homelab.io/storage-network: "true" }
      containers:
        - name: store
          image: "busybox:1.37"
          command: [sh, -c, "mkdir -p /www && dd if=/dev/zero of=/www/blob bs=1M count=100 && sync && exec httpd -f -p 8080 -h /www"]
          ports: [{ containerPort: 8080 }]
          volumeMounts: [{ name: data, mountPath: /www }]
  volumeClaimTemplates:
    - metadata: { name: data }
      spec: { accessModes: [ReadWriteOnce], storageClassName: longhorn, resources: { requests: { storage: 1Gi } } }
---
apiVersion: v1
kind: Service
metadata: { name: store }
spec:
  selector: { app: store }
  ports: [{ port: 80, targetPort: 8080 }]
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: store }
spec:
  rules:
    - http:
        paths: [{ path: /blob, pathType: Prefix, backend: { service: { name: store, port: { number: 80 } } } }]
`,
    ns,
  )
  try {
    const claim = `${ns}/data-store-0`
    const used = await waitFor('the claim to hold about 100 MiB', 6 * 60_000, () => {
      const v = watch.seen!.kube.volumes.find((x) => x.pvc === claim)
      return v?.used_bytes !== null && v?.used_bytes !== undefined && v.used_bytes >= 100 * 2 ** 20 ? v : null
    })
    note(`${claim}: ${Math.round(used.value.used_bytes! / 2 ** 20)} MiB used of ${used.value.size_bytes / 2 ** 30} GiB, ${secs(used.ms)} after applying`)
    const vip = new URL((await kube()).server).hostname
    let fetched = 0
    const load = (async () => {
      const until = now() + 60_000
      while (now() < until) {
        const r = await fetch(`http://${vip}/blob`).catch(() => null)
        if (r?.ok) fetched += (await r.arrayBuffer()).byteLength
      }
    })()
    const rate = await waitAsync("store's send rate in /api/kube/metrics", 90_000, async () => {
      const r = await fetch(`${BASE}/api/kube/metrics?namespace=${ns}&kind=StatefulSet&name=store`)
      const series = (await r.json()).series as Record<string, [number, number][]>
      const tx = series.tx_bps.at(-1)?.[1] ?? 0
      return tx > 1e6 ? tx : `tx ${tx} b/s`
    })
    await load
    note(`while ${Math.round(fetched / 2 ** 20)} MiB were fetched through the Ingress, store sent ${(rate.value / 1e6).toFixed(1)} Mb/s`)
  } finally {
    await k.delete(`/api/v1/namespaces/${ns}`)
  }
})

watch.close()
console.log('\n==== results')
for (const r of results) console.log(`${r.ok ? 'PASS' : 'FAIL'}  [${r.id}] ${r.name}`)
process.exit(results.every((r) => r.ok) ? 0 : 1)
