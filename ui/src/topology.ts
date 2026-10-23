import type { Alert } from './gen/Alert'
import type { HostObj } from './gen/HostObj'
import type { Issue } from './gen/Issue'
import type { LldpNeighbor } from './gen/LldpNeighbor'
import type { PortLoad } from './gen/PortLoad'
import type { SwitchObj } from './gen/SwitchObj'
import { MGMT_NIC } from './grouping'

export const DEFAULT_COLLAPSE_OVER = 24
/** Access-switch uplinks are SFP+ ports; observed switch ports carry no speed. */
export const UPLINK_MBPS = 10_000

export type NodeKind = 'core' | 'access' | 'host' | 'group'
export type EdgeState = 'up' | 'down' | 'mismatch'

export interface TopoNode {
  id: string
  /** Centre of the node. */
  x: number
  y: number
  kind: NodeKind
  label: string
  /** Groups only: how many hosts they stand for, and how many of those have a down or mismatched leg. */
  hosts?: number
  down?: number
  mismatched?: number
}

export interface TopoEdge {
  a: string
  b: string
  /** Busier direction over link capacity, 0 to 1. */
  utilisation: number
  state: EdgeState
  hostPort?: string
  switchPort?: string
}

export interface LayoutOpts {
  /** An access switch with more hosts than this is drawn as one group node. */
  collapseOver?: number
  /** Latest per-port throughput, from the ground truth. */
  ports?: PortLoad[]
}

export interface Layout {
  nodes: TopoNode[]
  edges: TopoEdge[]
}

export const NODE_W = 76
export const NODE_H = 30
const HOST_STEP_X = 84
const HOST_STEP_Y = 44
const HOST_COLS = 8
const BLOCK_GAP = 28
const GROUP_W = 150
const Y_CORE = 50
const Y_ACCESS = 170
const Y_HOSTS = 290
export const MARGIN = 60

const byText = (a: string, b: string) => a.localeCompare(b, 'en', { numeric: true })

/** Utilisation bands: 0-50%, 50-80%, over 80%. */
export type Band = 'low' | 'mid' | 'high'

export function utilisationBand(u: number): Band {
  return u > 0.8 ? 'high' : u > 0.5 ? 'mid' : 'low'
}

const isCore = (s: SwitchObj) => s.mlag_peer !== null || s.id.startsWith('core')

const portKey = (kind: 'h' | 's', node: string, port: string) => `${kind}:${node}/${port}`

function indexLoads(ports: PortLoad[]): Map<string, number> {
  const busiest = new Map<string, number>()
  for (const { port, rx_bps, tx_bps } of ports) {
    if (typeof port === 'string') continue
    const key = 'HostPort' in port ? portKey('h', ...port.HostPort) : portKey('s', ...port.SwitchPort)
    busiest.set(key, Math.max(rx_bps, tx_bps))
  }
  return busiest
}

/** Whether `issue` is about the cable that `leg` describes (see `Issue`). */
function concerns(issue: Issue, leg: LldpNeighbor, host: HostObj): boolean {
  if ('VlanNotAllowed' in issue || 'NativeVlanMismatch' in issue) {
    const i = 'VlanNotAllowed' in issue ? issue.VlanNotAllowed : issue.NativeVlanMismatch
    return i.switch === leg.switch && i.switch_port === leg.switch_port
  }
  if ('LacpNotConfigured' in issue) {
    return issue.LacpNotConfigured.switch === leg.switch && issue.LacpNotConfigured.port === leg.switch_port
  }
  if ('BondSplitWithoutMlag' in issue) {
    const config = host.net.desired ?? host.net.applied
    const link = config?.links.find((l) => l.name === issue.BondSplitWithoutMlag.link)
    return link !== undefined && 'Bond' in link.kind && link.kind.Bond.members.includes(leg.local_port)
  }
  return false
}

interface Leg {
  host: HostObj
  neighbour: LldpNeighbor
  utilisation: number
  state: EdgeState
}

function legOf(host: HostObj, neighbour: LldpNeighbor, switches: Map<string, SwitchObj>, loads: Map<string, number>): Leg {
  const nic = host.inventory.nics.find((n) => n.name === neighbour.local_port)
  const observed = switches.get(neighbour.switch)?.ports.find((p) => p.port === neighbour.switch_port)
  const down = !nic || !nic.link_up || observed?.cfg.admin_up === false
  const mismatch = host.net.issues.some((i) => concerns(i, neighbour, host))
  const capacity = (nic?.speed_mbps ?? 0) * 1e6
  const load = loads.get(portKey('h', host.id, neighbour.local_port)) ?? 0
  return {
    host,
    neighbour,
    utilisation: down || capacity === 0 ? 0 : Math.min(1, load / capacity),
    state: down ? 'down' : mismatch ? 'mismatch' : 'up',
  }
}

/** Worst of a group's legs: mismatch beats up, and a group is down only when all its legs are. */
function worst(legs: Leg[]): EdgeState {
  if (legs.every((l) => l.state === 'down')) return 'down'
  return legs.some((l) => l.state === 'mismatch') ? 'mismatch' : 'up'
}

/**
 * Lays the fabric out in three rows: cores on top, access switches below,
 * hosts under the access switch their mgmt NIC is cabled to. Hosts that have
 * not reported LLDP yet are left out. Access uplink `sfp<k>` is taken to go to
 * the k-th core by name, the way the presets cable them.
 */
export function layoutTopology(switches: SwitchObj[], hosts: HostObj[], opts: LayoutOpts = {}): Layout {
  const collapseOver = opts.collapseOver ?? DEFAULT_COLLAPSE_OVER
  const loads = indexLoads(opts.ports ?? [])
  const bySwitch = new Map(switches.map((s) => [s.id, s]))
  const cores = switches.filter(isCore).sort((a, b) => byText(a.id, b.id))
  const accesses = switches.filter((s) => !isCore(s)).sort((a, b) => byText(a.id, b.id))

  const hostsOf = new Map<string, HostObj[]>(accesses.map((s) => [s.id, []]))
  for (const h of [...hosts].sort((a, b) => byText(a.id, b.id))) {
    const mgmt = h.lldp.find((n) => n.local_port === MGMT_NIC)
    hostsOf.get(mgmt?.switch ?? '')?.push(h)
  }

  const nodes: TopoNode[] = []
  const edges: TopoEdge[] = []

  // Columns, left to right: one block per access switch.
  let cursor = MARGIN
  const accessX = new Map<string, number>()
  for (const sw of accesses) {
    const members = hostsOf.get(sw.id)!
    const collapsed = members.length > collapseOver
    const cols = collapsed ? 1 : Math.max(1, Math.min(HOST_COLS, members.length))
    const width = collapsed ? GROUP_W : cols * HOST_STEP_X
    const centre = cursor + width / 2
    accessX.set(sw.id, centre)
    nodes.push({ id: sw.id, x: centre, y: Y_ACCESS, kind: 'access', label: sw.id })

    const legs = members.flatMap((h) => h.lldp.filter((n) => bySwitch.has(n.switch)).map((n) => legOf(h, n, bySwitch, loads)))
    if (collapsed) {
      const gid = `group:${sw.id}`
      const flagged = (state: EdgeState) => members.filter((h) => legs.some((l) => l.host === h && l.state === state)).length
      nodes.push({
        id: gid,
        x: centre,
        y: Y_HOSTS,
        kind: 'group',
        label: `${members.length} hosts`,
        hosts: members.length,
        down: flagged('down'),
        mismatched: flagged('mismatch'),
      })
      // One edge to the access switch, and one per other switch the hosts' extra legs reach.
      for (const target of new Set(legs.map((l) => l.neighbour.switch))) {
        const group = legs.filter((l) => l.neighbour.switch === target)
        edges.push({
          a: gid,
          b: target,
          utilisation: Math.max(...group.map((l) => l.utilisation)),
          state: worst(group),
        })
      }
    } else {
      members.forEach((h, i) => {
        const x = cursor + (i % cols) * HOST_STEP_X + HOST_STEP_X / 2
        nodes.push({ id: h.id, x, y: Y_HOSTS + Math.floor(i / cols) * HOST_STEP_Y, kind: 'host', label: h.id })
      })
      for (const l of legs) {
        edges.push({
          a: l.host.id,
          b: l.neighbour.switch,
          utilisation: l.utilisation,
          state: l.state,
          hostPort: l.neighbour.local_port,
          switchPort: l.neighbour.switch_port,
        })
      }
    }
    cursor += width + BLOCK_GAP
  }

  const total = Math.max(cursor - BLOCK_GAP + MARGIN, 2 * MARGIN + cores.length * HOST_STEP_X)
  const coreStep = total / (cores.length + 1)
  cores.forEach((sw, i) => nodes.push({ id: sw.id, x: coreStep * (i + 1), y: Y_CORE, kind: 'core', label: sw.id }))

  // Uplinks: every access switch is dual-homed, `sfp<k>` to the k-th core.
  for (const sw of accesses) {
    cores.forEach((core, i) => {
      const port = `sfp${i + 1}`
      const observed = sw.ports.find((p) => p.port === port)
      const down = observed?.cfg.admin_up === false
      const load = loads.get(portKey('s', sw.id, port)) ?? 0
      edges.push({
        a: sw.id,
        b: core.id,
        utilisation: down ? 0 : Math.min(1, load / (UPLINK_MBPS * 1e6)),
        state: down ? 'down' : 'up',
        switchPort: port,
      })
    })
  }

  // Deterministic output: nodes by row then x, edges by endpoints.
  nodes.sort((p, q) => p.y - q.y || p.x - q.x || byText(p.id, q.id))
  edges.sort((p, q) => byText(p.a, q.a) || byText(p.b, q.b) || byText(p.hostPort ?? '', q.hostPort ?? ''))
  return { nodes, edges }
}

/** The host and NIC cabled to each port of switch `id`, from the hosts' LLDP. */
export function portPeers(id: string, hosts: HostObj[]): Map<string, { host: HostObj; nic: string }> {
  const peers = new Map<string, { host: HostObj; nic: string }>()
  for (const host of hosts) {
    for (const n of host.lldp) if (n.switch === id) peers.set(n.switch_port, { host, nic: n.local_port })
  }
  return peers
}

export interface SuggestedFix {
  host: string
  text: string
  /** The switch port the fix names, if it starts with `<switch> <port>:`. */
  port: string | null
}

/**
 * The switch-side fixes for `id`: from hosts cabled to it that have an open
 * `NetworkMismatch` alert, the host's fixes that start with this switch's id.
 */
export function suggestedFixes(id: string, hosts: HostObj[], alerts: Alert[]): SuggestedFix[] {
  const alerting = new Set(
    alerts.filter((a) => a.kind === 'NetworkMismatch' && a.resolved_at === null && a.entity.kind === 'Host').map((a) => a.entity.id),
  )
  return hosts
    .filter((h) => alerting.has(h.id) && h.lldp.some((n) => n.switch === id))
    .sort((a, b) => byText(a.id, b.id))
    .flatMap((h) =>
      h.net.fixes
        .filter((f) => f.startsWith(`${id} `))
        .map((text) => ({ host: h.id, text, port: /^(\S+):/.exec(text.slice(id.length + 1))?.[1] ?? null })),
    )
}

/** 1.5e9 -> "1.50 Gb/s". */
export function formatBps(bps: number): string {
  if (bps >= 1e9) return `${(bps / 1e9).toFixed(2)} Gb/s`
  if (bps >= 1e6) return `${(bps / 1e6).toFixed(1)} Mb/s`
  if (bps >= 1e3) return `${(bps / 1e3).toFixed(0)} kb/s`
  return `${Math.round(bps)} b/s`
}
