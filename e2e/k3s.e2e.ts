// End-to-end validation of the real K3s + Longhorn stack bootstrapped by
// homelabd on the lab VMs (design: docs/plans/2026-10-27-k3s-bootstrap-design.md §7).
//
// Talks to the server's WebSocket (homelab view + lab truth) and
// `POST /api/lab/fault` (hardware faults), and to K3s through
// `./lab.sh ssh <vm> 'sudo k3s kubectl ...'` (K3s's own public interface).
// Each step prints a pass/fail report with timings.
//
//   node e2e/k3s.e2e.ts [--only 2,5]     (LAB_URL defaults to http://127.0.0.1:28080)
//
// Expects a fresh lab (`./lab.sh reset` then `./lab.sh up`): 4 VMs that the
// run grows to 5 (step 6 racks one more, which the lab keeps).

import { execFileSync } from 'node:child_process'
import type { HostObj } from '../ui/src/gen/HostObj.ts'
import type { LabFault } from '../ui/src/gen/LabFault.ts'
import type { LabTruth } from '../ui/src/gen/LabTruth.ts'
import type { ServerMsg } from '../ui/src/gen/ServerMsg.ts'
import type { Alert } from '../ui/src/gen/Alert.ts'

const BASE = process.env.LAB_URL ?? 'http://127.0.0.1:28080'

interface Seen {
  leader: string | null
  hosts: Map<string, HostObj>
  alerts: Map<string, Alert>
  voters: string[]
  truth: LabTruth
}

class Watcher {
  seen: Seen | null = null
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
        hosts: new Map(m.view.hosts.map((h) => [h.id, h])),
        alerts: new Map(m.view.alerts.map((a) => [a.id, a])),
        voters: [...m.view.cp.voters],
        truth: m.truth,
      }
      return
    }
    const s = this.seen!
    s.leader = m.leader
    s.truth = m.truth
    for (const o of m.upserts) {
      if (o.type === 'Host') s.hosts.set(o.id, o)
      else if (o.type === 'Alert') s.alerts.set(o.id, o)
      else if (o.type === 'Cp') s.voters = [...o.voters]
    }
    for (const r of m.removed) {
      if (r.kind === 'Host') s.hosts.delete(r.id)
      else if (r.kind === 'Alert') s.alerts.delete(r.id)
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
const secs = (ms: number) => `${(ms / 1000).toFixed(1)} s`

class Failed extends Error {}

const K3S_SERVERS = ['ms01-01', 'ms01-02', 'ms01-03']

function sshRun(vm: string, cmd: string, timeoutMs = 60_000): string {
  return execFileSync('./lab.sh', ['ssh', vm, cmd], { encoding: 'utf8', timeout: timeoutMs }) as string
}

/** Runs `kubectl ...` on a K3s server that answers (tried in order). */
function kubectl(args: string, timeoutMs = 60_000): string {
  const errors: string[] = []
  for (const vm of K3S_SERVERS) {
    try {
      return sshRun(vm, `sudo k3s kubectl ${args}`, timeoutMs)
    } catch (e) {
      errors.push(`${vm}: ${String(e).split('\n')[0]}`)
    }
  }
  throw new Failed(`kubectl ${args}: none of ${K3S_SERVERS} answered (${errors.join('; ')})`)
}

/** Applies a manifest on the first answering server (via a guest-side file). */
function kubectlApply(manifest: string): string {
  const quoted = manifest
    .split('\n')
    .map((l) => `"${l.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`)
    .join(' ')
  const errors: string[] = []
  for (const vm of K3S_SERVERS) {
    try {
      sshRun(vm, `printf "%s\\n" ${quoted} > /tmp/k3s-e2e.yaml`)
      return sshRun(vm, 'sudo k3s kubectl apply -f /tmp/k3s-e2e.yaml')
    } catch (e) {
      errors.push(`${vm}: ${String(e).split('\n')[0]}`)
    }
  }
  throw new Failed(`kubectl apply: none of ${K3S_SERVERS} answered (${errors.join('; ')})`)
}

async function fault(f: LabFault): Promise<string | undefined> {
  const res = await fetch(`${BASE}/api/lab/fault`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(f) })
  if (!res.ok) throw new Failed(`fault ${JSON.stringify(f)}: ${res.status} ${await res.text()}`)
  return ((await res.json()) as { vm?: string }).vm
}

async function waitFor<T>(what: string, timeoutMs: number, check: (s: Seen) => T | null | undefined | false): Promise<{ value: T; ms: number }> {
  const start = now()
  for (;;) {
    if (watch.closed) throw new Failed(watch.closed)
    const v = watch.seen && check(watch.seen)
    if (v) return { value: v, ms: now() - start }
    if (now() - start > timeoutMs) throw new Failed(`timed out after ${timeoutMs / 1000} s waiting for ${what}`)
    await sleep(2000)
  }
}

/** Polls a kubectl-backed check until true. */
async function waitKubectl(what: string, timeoutMs: number, check: () => boolean): Promise<number> {
  const start = now()
  for (;;) {
    try {
      if (check()) return now() - start
    } catch {
      // A dead API server is "not yet", not failure.
    }
    if (now() - start > timeoutMs) throw new Failed(`timed out after ${timeoutMs / 1000} s waiting for ${what}`)
    await sleep(10000)
  }
}

function unhealthy(s: Seen): string | null {
  const vms = s.truth.vms
  if (vms.length === 0) return 'no lab machines'
  const off = vms.filter((v) => !v.installed || v.power !== 'Running')
  if (off.length) return `not running: ${off.map((v) => v.id).join(', ')}`
  const notReady = vms.filter((v) => s.hosts.get(v.id)?.lifecycle !== 'Ready')
  if (notReady.length) return `not Ready: ${notReady.map((v) => v.id).join(', ')}`
  if (s.truth.vip_holders.length !== 1) return `VIP held by [${s.truth.vip_holders}]`
  return null
}

async function healthy(timeoutMs: number) {
  let why: string | null = 'not checked'
  try {
    return await waitFor('a healthy homelab cluster', timeoutMs, (s) => (why = unhealthy(s)) === null)
  } catch (e) {
    throw new Failed(`${(e as Error).message} (last: ${why})`)
  }
}

/** Ready K3s nodes and their roles, from `kubectl get nodes`. */
function k3sNodes(): { name: string; ready: boolean; server: boolean }[] {
  return kubectl('get nodes --no-headers')
    .trim()
    .split('\n')
    .filter(Boolean)
    .map((l) => {
      const [name, status, roles] = l.split(/\s+/)
      return { name, ready: status === 'Ready', server: (roles ?? '').includes('control-plane') }
    })
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

await watch.ready

await step('1', 'Homelab up: every machine Ready, one VIP holder', async (note) => {
  const { ms } = await healthy(15 * 60_000)
  const s = watch.seen!
  note(`healthy after ${secs(ms)}: leader ${s.leader}, voters ${s.voters.join(', ')}`)
})

await step('2', 'K3s up: 3 servers + 1 agent Ready, etcd on the servers', async (note) => {
  const ms = await waitKubectl('4 Ready K3s nodes (3 servers)', 40 * 60_000, () => {
    const nodes = k3sNodes()
    return nodes.length === 4 && nodes.every((n) => n.ready) && nodes.filter((n) => n.server).length === 3
  })
  const nodes = k3sNodes()
  note(`4/4 Ready after ${secs(ms)}: ${nodes.map((n) => `${n.name}${n.server ? '(server)' : ''}`).join(', ')}`)
  const etcd = kubectl('get nodes -o jsonpath="{range .items[*]}{.metadata.name}={.metadata.labels.node-role\\.kubernetes\\.io/etcd}{\\"\\n\\"}{end}"')
  const members = etcd.trim().split('\n').filter((l) => l.endsWith('=true')).length
  if (members !== 3) throw new Failed(`expected 3 etcd members, saw ${members}: ${etcd}`)
  note(`etcd members: 3`)
})

await step('3', 'Longhorn up: pods Running, storage class present', async (note) => {
  const ms = await waitKubectl('Longhorn pods Running', 15 * 60_000, () => {
    const lines = kubectl('-n longhorn-system get pods --no-headers').trim().split('\n').filter(Boolean)
    return lines.length > 10 && lines.every((l) => l.split(/\s+/)[2] === 'Running')
  })
  const sc = kubectl('get storageclass longhorn --no-headers')
  if (!sc.includes('driver.longhorn.io')) throw new Failed(`no longhorn storage class: ${sc}`)
  note(`Longhorn Running after ${secs(ms)}; storageclass longhorn present`)
})

const NGINX = `apiVersion: apps/v1
kind: Deployment
metadata:
  name: k3s-e2e-web
spec:
  replicas: 3
  selector:
    matchLabels: { app: k3s-e2e-web }
  template:
    metadata:
      labels: { app: k3s-e2e-web }
    spec:
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
          - { weight: 100, podAffinityTerm: { labelSelector: { matchLabels: { app: k3s-e2e-web } }, topologyKey: kubernetes.io/hostname } }
      # Evict from a dead node after 60 s (stock K8s mechanism, faster than
      # the 5-minute default so the fault step observes rescheduling).
      tolerations:
      - { key: node.kubernetes.io/not-ready, operator: Exists, effect: NoExecute, tolerationSeconds: 60 }
      - { key: node.kubernetes.io/unreachable, operator: Exists, effect: NoExecute, tolerationSeconds: 60 }
      containers:
      - { name: web, image: nginx:1.27-alpine }
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: k3s-e2e-data
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: longhorn
  resources: { requests: { storage: 1Gi } }
---
apiVersion: v1
kind: Pod
metadata:
  name: k3s-e2e-writer
spec:
  restartPolicy: Never
  containers:
  - name: w
    image: busybox:1.36
    command: [sh, -c, "echo e2e-ok > /data/marker && cat /data/marker && sleep 3600"]
    volumeMounts: [{ name: vol, mountPath: /data }]
  volumes: [{ name: vol, persistentVolumeClaim: { claimName: k3s-e2e-data } }]`

await step('4', 'Workloads: spread pods + a 3-replica volume carrying data', async (note) => {
  kubectlApply(NGINX)
  const pods = await waitKubectl('3/3 nginx pods Ready', 10 * 60_000, () => {
    const line = kubectl('get deployment k3s-e2e-web --no-headers')
    return line.split(/\s+/)[1] === '3/3'
  })
  const wide = kubectl('get pods -l app=k3s-e2e-web -o wide --no-headers')
  const hosts = new Set(wide.trim().split('\n').map((l) => l.split(/\s+/)[6]))
  note(`3/3 nginx Ready after ${secs(pods)} on ${hosts.size} node(s): ${[...hosts].join(', ')}`)
  if (hosts.size < 2) throw new Failed(`pods did not spread (all on ${[...hosts]})`)
  const vol = await waitKubectl('the PVC Bound and the writer Running', 10 * 60_000, () => {
    const pvc = kubectl('get pvc k3s-e2e-data --no-headers')
    const pod = kubectl('get pod k3s-e2e-writer --no-headers')
    return pvc.split(/\s+/)[1] === 'Bound' && pod.split(/\s+/)[2] === 'Running'
  })
  const marker = kubectl('logs k3s-e2e-writer').trim()
  if (!marker.includes('e2e-ok')) throw new Failed(`writer produced ${JSON.stringify(marker)}`)
  const replicas = kubectl('-n longhorn-system get replicas.longhorn.io --no-headers')
    .trim()
    .split('\n')
    .filter(Boolean).length
  note(`volume Bound + data read back after ${secs(vol)}; Longhorn replicas: ${replicas}`)
  if (replicas < 3) throw new Failed(`expected 3 volume replicas, saw ${replicas}`)
})

await step('5', 'Faults: K3s survives a dead server and a dead replica node', async (note) => {
  await healthy(5 * 60_000)
  // A K3s server that is not the homelab leader (the API we drive stays
  // put) and does not run the writer pod (its logs stay readable throughout).
  const s0 = watch.seen!
  const writerNode = kubectl('get pod k3s-e2e-writer -o wide --no-headers').trim().split(/\s+/)[6]
  const victim = s0.voters.find((v) => v !== s0.leader && v.toLowerCase() !== writerNode.toLowerCase())!
  note(`victim ${victim} (writer runs on ${writerNode})`)
  const volumeName = kubectl(`get pvc k3s-e2e-data -o jsonpath='{.spec.volumeName}'`).trim()
  const robustness = () => kubectl(`-n longhorn-system get volumes.longhorn.io ${volumeName} -o jsonpath='{.status.robustness}'`).trim()
  // Rebuild promptly (stock default waits 10 min for a down node to return).
  kubectl('-n longhorn-system patch settings.longhorn.io replica-replenishment-wait-interval --type merge -p \'{"value":"60"}\'')
  note('replica replenishment wait set to 60 s (test-only tuning of the 10-minute default)')
  // A canary pinned (by preference) to the victim, so the run observes a
  // real eviction rather than whatever the nginx pods happened to do.
  kubectlApply(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: k3s-e2e-canary
spec:
  replicas: 1
  selector:
    matchLabels: { app: k3s-e2e-canary }
  template:
    metadata:
      labels: { app: k3s-e2e-canary }
    spec:
      affinity:
        nodeAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
          - { weight: 100, preference: { matchExpressions: [{ key: kubernetes.io/hostname, operator: In, values: [${victim.toLowerCase()}] }] } }
      tolerations:
      - { key: node.kubernetes.io/not-ready, operator: Exists, effect: NoExecute, tolerationSeconds: 60 }
      - { key: node.kubernetes.io/unreachable, operator: Exists, effect: NoExecute, tolerationSeconds: 60 }
      containers:
      - { name: web, image: nginx:1.27-alpine }`)
  const canaryNode = () => kubectl('get pods -l app=k3s-e2e-canary -o wide --no-headers').trim().split(/\s+/)[6] ?? ''
  await waitKubectl('the canary Running on the victim', 5 * 60_000, () => {
    const row = kubectl('get pods -l app=k3s-e2e-canary -o wide --no-headers').trim().split(/\s+/)
    return row[2] === 'Running' && row[6] === victim.toLowerCase()
  })
  note(`canary Running on ${victim}`)
  await fault({ PowerOff: { vm: victim } })
  const downAt = now()
  try {
    const gone = await waitKubectl(`${victim} NotReady in K3s`, 5 * 60_000, () =>
      k3sNodes().some((n) => n.name === victim.toLowerCase() && !n.ready),
    )
    note(`powered off ${victim}; K3s marks it NotReady after ${secs(gone)} (API answers throughout)`)
    const resched = await waitKubectl('nginx evicted and back to 3/3 on the survivors', 8 * 60_000, () => {
      const line = kubectl('get deployment k3s-e2e-web --no-headers')
      // Only Running pods count: evicted ones linger as Terminating until
      // the dead kubelet returns to confirm them.
      const running = kubectl('get pods -l app=k3s-e2e-web -o wide --no-headers')
        .trim()
        .split('\n')
        .filter((p) => p.split(/\s+/)[2] === 'Running')
      return line.split(/\s+/)[1] === '3/3' && running.length === 3 && running.every((p) => p.split(/\s+/)[6] !== victim.toLowerCase())
    })
    note(`nginx rescheduled to 3/3 off ${victim} after ${secs(resched)}`)
    const canaried = await waitKubectl('the canary evicted and Running off the victim', 8 * 60_000, () => {
      const row = kubectl('get pods -l app=k3s-e2e-canary -o wide --no-headers').trim().split(/\s+/)
      return row[2] === 'Running' && row[6] !== undefined && row[6] !== victim.toLowerCase()
    })
    note(`canary evicted off ${victim}, Running on ${canaryNode()} after ${secs(canaried)}`)
    const marker = kubectl('logs k3s-e2e-writer').trim()
    if (!marker.includes('e2e-ok')) throw new Failed(`volume unreadable with ${victim} down: ${JSON.stringify(marker)}`)
    note(`volume data still readable with ${victim} down (robustness ${robustness()})`)
    const rebuiltDown = await waitKubectl('the lost replica rebuilt on a survivor', 8 * 60_000, () => {
      const running = kubectl('-n longhorn-system get replicas.longhorn.io --no-headers')
        .trim()
        .split('\n')
        .filter(Boolean)
        .map((l) => l.split(/\s+/))
        .filter((w) => w[2] === 'running')
      return running.length === 3 && running.every((w) => w[3].toLowerCase() !== victim.toLowerCase())
    })
    note(`lost replica rebuilt on a survivor after ${secs(rebuiltDown)} (robustness ${robustness()})`)
  } finally {
    await fault({ PowerOn: { vm: victim } })
  }
  const back = await waitKubectl(`${victim} a Ready K3s node again`, 10 * 60_000, () =>
    k3sNodes().some((n) => n.name === victim.toLowerCase() && n.ready),
  )
  note(`${victim} back as a Ready K3s node after ${secs(back)}`)
  const rebuilt = await waitKubectl('the volume healthy', 10 * 60_000, () => robustness() === 'healthy')
  note(`volume Healthy again after ${secs(rebuilt)} (${secs(now() - downAt)} after the power-off)`)
  const heal = await healthy(5 * 60_000)
  note(`homelab healthy again after ${secs(heal.ms)}`)
})

await step('6', 'Join: a 5th VM joins homelab and K3s unaided', async (note) => {
  await healthy(5 * 60_000)
  const start = now()
  const id = await fault({ AddVm: { profile: 'Ms01', mistake: null } })
  if (!id) throw new Failed('AddVm named no machine')
  await waitFor(`${id} homelab-Ready`, 15 * 60_000, (s) => s.hosts.get(id)?.lifecycle === 'Ready')
  note(`${id} homelab-Ready ${secs(now() - start)} after the request`)
  const ms = await waitKubectl(`${id} a Ready K3s node`, 25 * 60_000, () => k3sNodes().some((n) => n.name === id.toLowerCase() && n.ready))
  note(`${id} a Ready K3s node ${secs(now() - start)} after the request (K3s join took ${secs(ms)} of it)`)
})

watch.close()
console.log('\n==== results')
for (const r of results) console.log(`${r.ok ? 'PASS' : 'FAIL'}  [${r.id}] ${r.name}`)
process.exit(results.every((r) => r.ok) ? 0 : 1)
