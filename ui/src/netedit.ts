import type { Attachment } from './gen/Attachment'
import type { BondMode } from './gen/BondMode'
import type { HostNetworkConfig } from './gen/HostNetworkConfig'
import type { Issue } from './gen/Issue'
import type { IpRange } from './gen/IpRange'
import type { Link } from './gen/Link'
import type { LinkKind } from './gen/LinkKind'
import type { LogicalNetwork } from './gen/LogicalNetwork'
import type { NetworkPurpose } from './gen/NetworkPurpose'

/** Pure edits of a host network config. Each returns a new config and leaves its input alone. */

/** The ports a link uses: its NIC, or a bond's members. */
export function linkPorts(kind: LinkKind): string[] {
  return 'Single' in kind ? [kind.Single] : kind.Bond.members
}

/** Every port some link uses, mapped to that link's name. */
export function portOwners(cfg: HostNetworkConfig): Map<string, string> {
  return new Map(cfg.links.flatMap((l) => linkPorts(l.kind).map((p) => [p, l.name] as const)))
}

/** Adds a link on one port, unless a link uses that port already. */
export function addSingle(cfg: HostNetworkConfig, port: string, mtu = 1500): HostNetworkConfig {
  if (portOwners(cfg).has(port)) return cfg
  const taken = new Set(cfg.links.map((l) => l.name))
  const name = taken.has(port) ? `${port}-link` : port
  return { ...cfg, links: [...cfg.links, { name, kind: { Single: port }, mtu }] }
}

/**
 * Bonds `members` into a new link `name`. The members' single links go away and their
 * attachments move onto the bond (the first one wins when two carry the same network).
 */
export function addBond(cfg: HostNetworkConfig, name: string, members: string[], mode: BondMode): HostNetworkConfig {
  const absorbed = cfg.links.filter((l) => 'Single' in l.kind && members.includes(l.kind.Single))
  const gone = new Set(absorbed.map((l) => l.name))
  const mtu = absorbed.length > 0 ? Math.max(...absorbed.map((l) => l.mtu)) : 1500
  const bond: Link = { name, kind: { Bond: { members, mode } }, mtu }
  const seen = new Set<string>()
  const attachments: Attachment[] = []
  for (const a of cfg.attachments) {
    const moved = gone.has(a.link) ? { ...a, link: name } : a
    if (moved.link === name) {
      if (seen.has(moved.network)) continue
      seen.add(moved.network)
    }
    attachments.push(moved)
  }
  return { links: [...cfg.links.filter((l) => !gone.has(l.name)), bond], attachments }
}

/** Moves a network's attachment onto another link, keeping its address and tagging. */
export function moveAttachment(cfg: HostNetworkConfig, network: string, link: string): HostNetworkConfig {
  return { ...cfg, attachments: cfg.attachments.map((a) => (a.network === network ? { ...a, link } : a)) }
}

export function setTagged(cfg: HostNetworkConfig, network: string, tagged: boolean): HostNetworkConfig {
  return { ...cfg, attachments: cfg.attachments.map((a) => (a.network === network ? { ...a, tagged } : a)) }
}

/** `ip` of null or blank clears the address. */
export function setIp(cfg: HostNetworkConfig, network: string, ip: string | null): HostNetworkConfig {
  const value = ip === null || ip.trim() === '' ? null : ip.trim()
  return { ...cfg, attachments: cfg.attachments.map((a) => (a.network === network ? { ...a, ip: value } : a)) }
}

export function setMtu(cfg: HostNetworkConfig, link: string, mtu: number): HostNetworkConfig {
  return { ...cfg, links: cfg.links.map((l) => (l.name === link ? { ...l, mtu } : l)) }
}

/** Attaches a network to a link, unless the network is attached already. */
export function addAttachment(cfg: HostNetworkConfig, network: string, link: string, tagged: boolean): HostNetworkConfig {
  if (cfg.attachments.some((a) => a.network === network)) return cfg
  return { ...cfg, attachments: [...cfg.attachments, { network, link, tagged, ip: null }] }
}

export function removeAttachment(cfg: HostNetworkConfig, network: string): HostNetworkConfig {
  return { ...cfg, attachments: cfg.attachments.filter((a) => a.network !== network) }
}

/** Removes a link and the attachments on it. */
export function removeLink(cfg: HostNetworkConfig, name: string): HostNetworkConfig {
  return { links: cfg.links.filter((l) => l.name !== name), attachments: cfg.attachments.filter((a) => a.link !== name) }
}

/** Problems that can be seen without the cluster's state; the server validates the rest. */
export function formProblems(cfg: HostNetworkConfig): string[] {
  const out: string[] = []
  const names = new Set<string>()
  const ports = new Set<string>()
  for (const l of cfg.links) {
    if (l.name.trim() === '') out.push('A link has no name')
    if (names.has(l.name)) out.push(`Link name ${l.name} is used twice`)
    names.add(l.name)
    for (const p of linkPorts(l.kind)) {
      if (ports.has(p)) out.push(`Port ${p} is on two links`)
      ports.add(p)
    }
    if ('Bond' in l.kind && l.kind.Bond.members.length < 2) out.push(`Bond ${l.name} needs at least two ports`)
    if (!(l.mtu >= 576 && l.mtu <= 9216)) out.push(`Link ${l.name}: MTU ${l.mtu} is out of range (576-9216)`)
  }
  for (const a of cfg.attachments) {
    if (!names.has(a.link)) out.push(`Network ${a.network} is on a link that does not exist`)
  }
  return out
}

function describeKind(kind: LinkKind): string {
  return 'Single' in kind ? kind.Single : `${kind.Bond.mode} bond of ${kind.Bond.members.join('+')}`
}

/**
 * What differs between two configs, as sentences. Addresses are left out: a template has
 * none, they are allocated per host.
 */
export function configDiff(base: HostNetworkConfig, other: HostNetworkConfig): string[] {
  const out: string[] = []
  const baseLinks = new Map(base.links.map((l) => [l.name, l]))
  const otherLinks = new Map(other.links.map((l) => [l.name, l]))
  for (const [name, l] of otherLinks) {
    const b = baseLinks.get(name)
    if (!b) out.push(`link ${name} added (${describeKind(l.kind)}, MTU ${l.mtu})`)
    else {
      if (describeKind(b.kind) !== describeKind(l.kind)) out.push(`link ${name}: ${describeKind(b.kind)} -> ${describeKind(l.kind)}`)
      if (b.mtu !== l.mtu) out.push(`link ${name}: MTU ${b.mtu} -> ${l.mtu}`)
    }
  }
  for (const name of baseLinks.keys()) if (!otherLinks.has(name)) out.push(`link ${name} removed`)
  const baseAtt = new Map(base.attachments.map((a) => [a.network, a]))
  const otherAtt = new Map(other.attachments.map((a) => [a.network, a]))
  for (const [network, a] of otherAtt) {
    const b = baseAtt.get(network)
    if (!b) out.push(`${network} attached to ${a.link}${a.tagged ? ' (tagged)' : ''}`)
    else {
      if (b.link !== a.link) out.push(`${network}: link ${b.link} -> ${a.link}`)
      if (b.tagged !== a.tagged) out.push(`${network}: ${b.tagged ? 'tagged' : 'untagged'} -> ${a.tagged ? 'tagged' : 'untagged'}`)
    }
  }
  for (const network of baseAtt.keys()) if (!otherAtt.has(network)) out.push(`${network} detached`)
  return out
}

/**
 * The users named by an `in use by ...` refusal ("in use by template a, rule b" gives
 * ["template a", "rule b"]); null when the text is something else.
 */
export function parseInUse(error: string): string[] | null {
  const m = /\bin use by (.+)$/.exec(error.trim())
  if (!m) return null
  const users = m[1].split(',').map((u) => u.trim()).filter((u) => u !== '')
  return users.length > 0 ? users : null
}

/** One sentence for a validation issue. */
export function describeIssue(issue: Issue): string {
  const on = (networks: string[]) => (networks.length > 0 ? ` (carries ${networks.join(', ')})` : '')
  if ('MissingRequiredNetwork' in issue) {
    const { role, network } = issue.MissingRequiredNetwork
    return `${role} needs network ${network}, which is not attached`
  }
  if ('VlanNotAllowed' in issue) {
    const i = issue.VlanNotAllowed
    return `${i.network}: ${i.switch} ${i.switch_port} does not allow VLAN ${i.vlan} (host port ${i.host_port})`
  }
  if ('NativeVlanMismatch' in issue) {
    const i = issue.NativeVlanMismatch
    return `${i.network}: untagged on ${i.host_port}, but ${i.switch} ${i.switch_port} has native VLAN ${i.native ?? 'none'}, not ${i.vlan}`
  }
  if ('MtuTooSmall' in issue) {
    const i = issue.MtuTooSmall
    return `${i.network}: MTU ${i.have} at ${i.where_}, needs ${i.need}`
  }
  if ('BondSplitWithoutMlag' in issue) {
    const i = issue.BondSplitWithoutMlag
    return `bond ${i.link} spans switches that are not an MLAG pair${on(i.networks)}`
  }
  if ('LacpNotConfigured' in issue) {
    const i = issue.LacpNotConfigured
    return `${i.link}: ${i.switch} ${i.port} has no LACP group${on(i.networks)}`
  }
  if ('IpConflict' in issue) {
    const i = issue.IpConflict
    return `${i.network}: ${i.ip} is also used by ${i.other_host}`
  }
  if ('PortNotCabled' in issue) {
    const i = issue.PortNotCabled
    return `port ${i.port} is not cabled${on(i.networks)}`
  }
  return `network ${issue.UnknownNetwork.id} does not exist`
}

export interface NetworkForm {
  id: string
  purpose: NetworkPurpose
  vlan: string
  subnet: string
  mtu: string
  /** Not edited in the form: carried through so an edit keeps the network's reservations. */
  reserved?: IpRange[]
}

export function formFromNetwork(n: LogicalNetwork): NetworkForm {
  return { id: n.id, purpose: n.purpose, vlan: String(n.vlan), subnet: n.subnet, mtu: String(n.mtu), reserved: n.reserved }
}

const CIDR = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})\/(\d{1,2})$/

/** A logical network from its form, or the first problem with a field. */
export function networkFromForm(f: NetworkForm): { network: LogicalNetwork } | { error: string } {
  const id = f.id.trim()
  if (id === '') return { error: 'Name: required' }
  const vlan = Number(f.vlan)
  if (!Number.isInteger(vlan) || vlan < 1 || vlan > 4094) return { error: `VLAN: "${f.vlan}" is not a VLAN id (1-4094)` }
  const subnet = f.subnet.trim()
  const m = CIDR.exec(subnet)
  if (!m || m.slice(1, 5).some((o) => Number(o) > 255) || Number(m[5]) > 30) return { error: `Subnet: "${f.subnet}" is not an IPv4 CIDR like 10.0.0.0/22` }
  const mtu = Number(f.mtu)
  if (!Number.isInteger(mtu) || mtu < 576 || mtu > 9216) return { error: `MTU: "${f.mtu}" is not between 576 and 9216` }
  return { network: { id, purpose: f.purpose, vlan, subnet, mtu, reserved: f.reserved ?? [] } }
}
