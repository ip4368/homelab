// End-to-end validation of the cluster manager on the real VM lab.
//
// Talks only to the server's public interfaces: the WebSocket (the control
// plane's view and the lab's ground truth) and `POST /api/lab/fault` (things
// done to hardware). Each step is a success criterion of the real VM lab
// design (docs/plans/2026-10-26-real-vm-lab-design.md §10), timed.
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
})

await step('3a', 'Leader failover: power off the leader', async (note) => {
  await healthy(5 * 60_000)
  const old = watch.seen!.leader!
  const term = watch.seen!.term
  await fault({ PowerOff: { vm: old } })
  const elected = await waitFor('a new leader in the client view', 30_000, (s) => s.leader !== null && s.leader !== old && s.leader)
  note(`powered off ${old}; the client saw ${elected.value} lead (term ${watch.seen!.term} > ${term}) after ${secs(elected.ms)}`)
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

watch.close()
console.log('\n==== results')
for (const r of results) console.log(`${r.ok ? 'PASS' : 'FAIL'}  [${r.id}] ${r.name}`)
process.exit(results.every((r) => r.ok) ? 0 : 1)
