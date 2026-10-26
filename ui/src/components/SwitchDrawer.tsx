import { useState } from 'react'
import { labFault } from '../api'
import { formatClock } from '../clock'
import type { SwitchPortCfg } from '../gen/SwitchPortCfg'
import { formatBps, portPeers, suggestedFixes } from '../topology'
import { useStore } from '../store'
import { useAction } from './useAction'

export interface PortForm {
  mode: 'Access' | 'Trunk'
  access: string
  native: string
  allowed: string
  mtu: string
  lacp: string
  adminUp: boolean
}

export function formFromCfg(cfg: SwitchPortCfg): PortForm {
  const trunk = 'Trunk' in cfg.mode ? cfg.mode.Trunk : null
  return {
    mode: trunk ? 'Trunk' : 'Access',
    access: 'Access' in cfg.mode ? String(cfg.mode.Access) : '1',
    native: trunk?.native != null ? String(trunk.native) : '',
    allowed: trunk ? trunk.allowed.join(', ') : '',
    mtu: String(cfg.mtu),
    lacp: cfg.lacp_group ?? '',
    adminUp: cfg.admin_up,
  }
}

function vlan(text: string, what: string): number {
  const n = Number(text)
  if (!Number.isInteger(n) || n < 1 || n > 4094) throw `${what}: "${text.trim()}" is not a VLAN id (1-4094)`
  return n
}

/** The port config a form describes, or the reason it does not describe one. */
export function cfgFromForm(f: PortForm): { cfg: SwitchPortCfg } | { error: string } {
  try {
    const mtu = Number(f.mtu)
    if (!Number.isInteger(mtu) || mtu < 576 || mtu > 9216) throw `MTU: "${f.mtu.trim()}" is not between 576 and 9216`
    const mode: SwitchPortCfg['mode'] =
      f.mode === 'Access'
        ? { Access: vlan(f.access, 'Access VLAN') }
        : {
            Trunk: {
              native: f.native.trim() === '' ? null : vlan(f.native, 'Native VLAN'),
              allowed: [...new Set(f.allowed.split(/[\s,]+/).filter(Boolean).map((t) => vlan(t, 'Allowed VLANs')))].sort((a, b) => a - b),
            },
          }
    return { cfg: { mode, mtu, lacp_group: f.lacp.trim() === '' ? null : f.lacp.trim(), admin_up: f.adminUp } }
  } catch (e) {
    return { error: String(e) }
  }
}

export function describeMode(mode: SwitchPortCfg['mode']): string {
  if ('Access' in mode) return `access ${mode.Access}`
  const { native, allowed } = mode.Trunk
  return `trunk native ${native ?? 'none'} allow ${allowed.join(',') || 'none'}`
}

/** The human editing a switch port: posts the lab fault `SetSwitchPort` (the control plane never configures switches). */
function PortEditor({ switchId, port, cfg }: { switchId: string; port: string; cfg: SwitchPortCfg }) {
  const [form, setForm] = useState(formFromCfg(cfg))
  const [invalid, setInvalid] = useState<string | null>(null)
  const [sent, setSent] = useState(false)
  const { error, busy, run } = useAction()
  const set = (patch: Partial<PortForm>) => {
    setForm({ ...form, ...patch })
    setSent(false)
  }
  const submit = () => {
    const parsed = cfgFromForm(form)
    setInvalid('error' in parsed ? parsed.error : null)
    if ('error' in parsed) return
    void run(async () => {
      await labFault({ SetSwitchPort: { switch: switchId, port, cfg: parsed.cfg } })
      setSent(true)
    })
  }
  return (
    <div className="section port-editor">
      <h4>
        Edit {switchId} {port}
      </h4>
      <div className="form-grid">
        <label>Mode</label>
        <select value={form.mode} onChange={(e) => set({ mode: e.target.value as PortForm['mode'] })}>
          <option>Access</option>
          <option>Trunk</option>
        </select>
        {form.mode === 'Access' ? (
          <>
            <label>VLAN</label>
            <input value={form.access} onChange={(e) => set({ access: e.target.value })} size={6} />
          </>
        ) : (
          <>
            <label>Native</label>
            <input value={form.native} onChange={(e) => set({ native: e.target.value })} placeholder="none" size={6} />
            <label>Allowed</label>
            <input value={form.allowed} onChange={(e) => set({ allowed: e.target.value })} placeholder="1, 20, 30" />
          </>
        )}
        <label>MTU</label>
        <input value={form.mtu} onChange={(e) => set({ mtu: e.target.value })} size={6} />
        <label>LACP group</label>
        <input value={form.lacp} onChange={(e) => set({ lacp: e.target.value })} placeholder="none" size={10} />
        <label>Admin</label>
        <label className="inline">
          <input type="checkbox" checked={form.adminUp} onChange={(e) => set({ adminUp: e.target.checked })} /> up
        </label>
      </div>
      <div className="actions">
        <button onClick={submit} disabled={busy}>
          Apply to switch
        </button>
        {sent && <span className="muted">Applied. The control plane sees it at the next switch poll.</span>}
        {(invalid ?? error) && <span className="bad">{invalid ?? error}</span>}
      </div>
    </div>
  )
}

interface DrawerProps {
  id: string
  onClose(): void
  onSelectHost(id: string): void
  onSelectSwitch(id: string): void
}

export function SwitchDrawer({ id, onClose, onSelectHost, onSelectSwitch }: DrawerProps) {
  const sw = useStore((s) => s.view.switches[id])
  const hostMap = useStore((s) => s.view.hosts)
  const alerts = useStore((s) => s.view.alerts)
  const loads = useStore((s) => s.truth.ports)
  const [port, setPort] = useState<string | null>(null)
  const [showAll, setShowAll] = useState(false)
  if (!sw) return null

  const hosts = Object.values(hostMap)
  const peers = portPeers(id, hosts)
  const load = new Map(
    loads.flatMap((l) => (l.switch === id && (l.rx_bps > 0 || l.tx_bps > 0) ? [[l.port, l] as const] : [])),
  )
  // A port stands out if it is cabled, busy, admin-down or configured unlike most of the switch.
  const configs = sw.ports.map((p) => JSON.stringify(p.cfg))
  const usual = [...configs].sort((a, b) => configs.filter((c) => c === b).length - configs.filter((c) => c === a).length)[0]
  const rows = [...sw.ports].sort((a, b) => a.port.localeCompare(b.port, 'en', { numeric: true })).filter((p) => showAll || peers.has(p.port) || p.neighbor !== null || load.has(p.port) || !p.cfg.admin_up || JSON.stringify(p.cfg) !== usual)
  const fixes = suggestedFixes(id, hosts, Object.values(alerts))
  const editing = sw.ports.find((p) => p.port === port)

  const linkOf = (p: (typeof sw.ports)[number]) => {
    if (!p.cfg.admin_up) return { text: 'admin down', cls: 'bad' }
    if (p.link_up) return { text: `up ${p.speed_mbps / 1000} Gb/s`, cls: '' }
    return peers.has(p.port) || p.neighbor ? { text: 'down', cls: 'bad' } : { text: '-', cls: '' }
  }

  return (
    <aside className="drawer switch-drawer" aria-label={`switch ${id}`}>
      <div className="drawer-head">
        <h3>{id}</h3>
        <span className="muted">{sw.model}</span>
        <div className="spacer" />
        <button onClick={onClose} aria-label="close">
          ×
        </button>
      </div>
      <p className="muted">
        {sw.mlag_peer ? `MLAG peer ${sw.mlag_peer}. ` : ''}Last seen at {formatClock(sw.last_seen)}. Ports show the last poll.
      </p>
      {fixes.length > 0 && (
        <div className="section">
          <h4>Suggested fixes</h4>
          <ul className="fixes">
            {fixes.map((f) => (
              <li key={`${f.host}:${f.text}`}>
                {f.port ? (
                  <button className="link" onClick={() => setPort(f.port)} title="Edit this port">
                    {f.text}
                  </button>
                ) : (
                  f.text
                )}{' '}
                <span className="muted">({f.host})</span>
              </li>
            ))}
          </ul>
        </div>
      )}
      <div className="section">
        <h4>
          Ports{' '}
          <label className="inline">
            <input type="checkbox" checked={showAll} onChange={(e) => setShowAll(e.target.checked)} /> show all {sw.ports.length}
          </label>
        </h4>
        <table className="ports">
          <thead>
            <tr>
              <th>Port</th>
              <th>Config</th>
              <th>Link</th>
              <th>Rx / Tx</th>
              <th>Host</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((p) => {
              const link = linkOf(p)
              const l = load.get(p.port)
              const peer = peers.get(p.port)
              return (
                <tr key={p.port} className={p.port === port ? 'selected' : ''}>
                  <td>
                    <button className="link" onClick={() => setPort(p.port)} title="Edit this port">
                      {p.port}
                    </button>
                  </td>
                  <td>
                    {describeMode(p.cfg.mode)}, mtu {p.cfg.mtu}
                    {p.cfg.lacp_group ? `, lacp ${p.cfg.lacp_group}` : ''}
                  </td>
                  <td className={`num ${link.cls}`}>{link.text}</td>
                  <td className="num">{l ? `${formatBps(l.rx_bps)} / ${formatBps(l.tx_bps)}` : '-'}</td>
                  <td>
                    {peer ? (
                      <button className="link" onClick={() => onSelectHost(peer.host.id)}>
                        {peer.host.id}
                      </button>
                    ) : p.neighbor ? (
                      <button className="link" onClick={() => onSelectSwitch(p.neighbor!.switch)}>
                        {p.neighbor.switch}
                      </button>
                    ) : (
                      ''
                    )}
                    {peer && <span className="muted"> {peer.nic}</span>}
                    {p.neighbor && <span className="muted"> {p.neighbor.port}</span>}
                  </td>
                </tr>
              )
            })}
            {rows.length === 0 && (
              <tr>
                <td colSpan={5} className="muted">
                  No cabled or busy ports.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      {editing ? (
        // Re-mount when the switch reports a different config for the port.
        <PortEditor key={`${id}:${editing.port}:${JSON.stringify(editing.cfg)}`} switchId={id} port={editing.port} cfg={editing.cfg} />
      ) : (
        <p className="muted">Pick a port to edit its config.</p>
      )}
    </aside>
  )
}
