import { Fragment, useMemo, useState } from 'react'
import { labFault, seedWorkloads } from '../api'
import type { LabFault } from '../gen/LabFault'
import type { LabMember } from '../gen/LabMember'
import type { Mistake } from '../gen/Mistake'
import type { Power } from '../gen/Power'
import type { ProfileId } from '../gen/ProfileId'
import type { VmTruth } from '../gen/VmTruth'
import { nav } from '../nav'
import { useStore } from '../store'
import { Card } from './DetailPane'
import { Menu } from './Menu'
import { useAction } from './useAction'

/** The machines the lab can rack. */
const PROFILES: ProfileId[] = ['Ms01', 'MiniPc']

/** The switch-port mistakes the technician can make on a new machine's ports. */
export const MISTAKES = [
  { value: 'none', label: 'configured correctly', mistake: null },
  { value: 'MissingStorageVlan', label: 'VLAN 20 missing', mistake: 'MissingStorageVlan' },
  { value: 'MtuTooSmall', label: 'MTU too small', mistake: 'MtuTooSmall' },
  { value: 'NoLacp', label: 'bond without LACP', mistake: 'NoLacp' },
] as const satisfies readonly { value: string; label: string; mistake: Mistake | null }[]

export function mistakeOf(value: string): Mistake | null {
  return MISTAKES.find((m) => m.value === value)?.mistake ?? null
}

/** One partition group: the switch (if any) and the machines listed, separated by commas or spaces. */
export function partitionOf(hosts: string, sw: string): LabFault | null {
  const ids = [...new Set(hosts.split(/[\s,]+/).filter((h) => h !== ''))]
  const members: LabMember[] = [...(sw.trim() ? [{ Switch: sw.trim() }] : []), ...ids.map((h) => ({ Host: h }))]
  return members.length > 0 ? { Partition: { members } } : null
}

const POWER_LABEL: Record<Power, string> = { Off: 'off', Running: 'running', Paused: 'frozen', Unknown: 'not answering' }
const POWER_TONE: Record<Power, string> = { Off: 'bad', Running: 'ok', Paused: 'warn', Unknown: 'bad' }

function powerPill(v: VmTruth) {
  return v.installed ? <span className={`pill ${POWER_TONE[v.power]}`}>{POWER_LABEL[v.power]}</span> : <span className="pill muted">not installed</span>
}

/** Power on, shut down, pull the plug, reset, freeze, thaw or partition one machine. */
function PowerActions({ v, compact = false }: { v: VmTruth; compact?: boolean }) {
  const { error, busy, run } = useAction()
  const send = (f: LabFault) => run(() => labFault(f))
  const vm = v.id
  const power = v.power
  const items = [
    { label: 'Power on', onClick: () => send({ PowerOn: { vm } }), disabled: busy || !v.installed || power !== 'Off' },
    { label: 'Shut down', onClick: () => send({ Shutdown: { vm } }), disabled: busy || power !== 'Running', title: 'Press the power button: a clean shutdown' },
    { label: 'Power off', onClick: () => send({ PowerOff: { vm } }), disabled: busy || power === 'Off', title: 'Pull the plug', danger: true },
    { label: 'Reset', onClick: () => send({ Reset: { vm } }), disabled: busy || power !== 'Running', title: 'Press reset: an instant reboot' },
    { label: 'Freeze', onClick: () => send({ Freeze: { vm } }), disabled: busy || power !== 'Running', title: 'Stop its CPUs: it sees no time pass' },
    { label: 'Thaw', onClick: () => send({ Thaw: { vm } }), disabled: busy || power !== 'Paused' },
    { label: 'Partition alone', onClick: () => send({ Partition: { members: [{ Host: vm }] } }), disabled: busy, title: 'Cut it off from every other machine and switch' },
  ]
  if (compact) {
    // The likeliest action as a button; the rest behind the menu.
    const main = power === 'Off' ? items[0] : power === 'Paused' ? items[5] : items[1]
    return (
      <span className="row-buttons">
        <button className="small" disabled={main.disabled} title={main.title} onClick={main.onClick}>
          {main.label}
        </button>
        <Menu label={`power actions for ${vm}`} items={items.filter((i) => i !== main)} />
        {error && <span className="bad">{error}</span>}
      </span>
    )
  }
  return (
    <div className="actions">
      {items.map((i) => (
        <button key={i.label} disabled={i.disabled} title={i.title} onClick={i.onClick} className={i.danger ? 'danger' : undefined}>
          {i.label}
        </button>
      ))}
      {error && <span className="bad">{error}</span>}
    </div>
  )
}

/**
 * One lab machine's controls, as a technician would have them: its power,
 * each NIC's cable and speed, and each data disk.
 */
export function MachineControls({ vm, showTruth = true }: { vm: string; showTruth?: boolean }) {
  const truth = useStore((s) => s.truth)
  const [mbps, setMbps] = useState(100)
  const { error, busy, run } = useAction()
  const v = truth.vms.find((x) => x.id === vm)
  const ports = useMemo(() => truth.ports.filter((p) => p.host === vm && p.nic !== null), [truth.ports, vm])
  if (!v) return <p className="muted">{vm} is not a lab machine.</p>
  const send = (f: LabFault) => run(() => labFault(f))
  // Degrading reaches into the guest unless it is off: not while frozen or not answering.
  const canDegrade = v.power === 'Running' || v.power === 'Off'
  return (
    <div className="card-grid machine-grid">
      <Card title="Power" extra={powerPill(v)} className="span-all">
        <PowerActions v={v} />
        {showTruth && (
          <p className="muted">
            {v.holds_vip ? 'Holds the VIP. ' : ''}
            {v.clock_offset_ms !== null && `Clock ${v.clock_offset_ms >= 0 ? '+' : ''}${v.clock_offset_ms} ms against the lab host. `}
            {v.uptime_s !== null && `Up ${Math.round(v.uptime_s / 60)} min. `}
            {v.installer && `Technician: ${v.installer}.`}
          </p>
        )}
      </Card>
      <Card
        title="Cables"
        extra={
          <label className="inline">
            degrade to{' '}
            <input type="number" min={1} value={mbps} onChange={(e) => setMbps(Math.max(1, Math.floor(Number(e.target.value)) || 1))} style={{ width: 70 }} aria-label="degraded port speed" /> Mb/s
          </label>
        }
      >
        <table className="grid-table compact">
          <tbody>
            {ports.map((p) => {
              const nic = p.nic as string
              const unplugged = v.unplugged.includes(nic)
              return (
                <tr key={nic}>
                  <td>
                    <strong>{nic}</strong>
                    <div className="cell-sub">
                      {p.switch} {p.port}
                    </div>
                  </td>
                  <td>
                    <span className={`pill ${unplugged ? 'bad' : p.link_up ? 'ok' : 'muted'}`}>{unplugged ? 'unplugged' : p.link_up ? 'link up' : 'link down'}</span>
                  </td>
                  <td className="row-actions">
                    {unplugged ? (
                      <button className="small" disabled={busy} onClick={() => send({ Replug: { vm, nic } })}>
                        Replug
                      </button>
                    ) : (
                      <button className="small" disabled={busy} onClick={() => send({ Unplug: { vm, nic } })}>
                        Unplug
                      </button>
                    )}
                    <button className="small" disabled={busy || !canDegrade} onClick={() => send({ Degrade: { vm, nic, mbps } })} title="The switch port carries at most this speed (like a bad cable)">
                      Degrade
                    </button>
                    <button className="small" disabled={busy || !canDegrade} onClick={() => send({ Degrade: { vm, nic, mbps: null } })}>
                      Full speed
                    </button>
                  </td>
                </tr>
              )
            })}
            {ports.length === 0 && (
              <tr>
                <td className="muted">No cabled NICs.</td>
              </tr>
            )}
          </tbody>
        </table>
      </Card>
      <Card title="Data disks">
        <table className="grid-table compact">
          <tbody>
            {v.disks.map((d) => {
              const failed = v.failed_disks.includes(d)
              return (
                <tr key={d}>
                  <td>
                    <strong>{d}</strong>
                  </td>
                  <td>
                    <span className={`pill ${failed ? 'bad' : 'ok'}`}>{failed ? 'pulled' : 'in'}</span>
                  </td>
                  <td className="row-actions">
                    {failed ? (
                      <button className="small" disabled={busy} onClick={() => send({ RestoreDisk: { vm, disk: d } })} title="Put the pulled disk back, with what was on it">
                        Restore disk
                      </button>
                    ) : (
                      <button className="small" disabled={busy} onClick={() => send({ FailDisk: { vm, disk: d } })} title="Pull the data disk, as if it died">
                        Fail disk
                      </button>
                    )}
                  </td>
                </tr>
              )
            })}
            {v.disks.length === 0 && (
              <tr>
                <td className="muted">No data disks.</td>
              </tr>
            )}
          </tbody>
        </table>
      </Card>
      {error && <div className="bad span-all">{error}</div>}
    </div>
  )
}

function memberLabel(m: LabMember): string {
  return 'Host' in m ? m.Host : `switch ${m.Switch}`
}

/**
 * The lab: its machines as a table (power them, open one's cables and disks,
 * pick some to partition), its switches, partitions, racking a new machine
 * and the demo workloads.
 */
export function LabPage() {
  const truth = useStore((s) => s.truth)
  const hosts = useStore((s) => s.view.hosts)
  const [expanded, setExpanded] = useState<string | null>(null)
  const [picked, setPicked] = useState<string[]>([])
  const [withSwitch, setWithSwitch] = useState('')
  const [profile, setProfile] = useState<ProfileId>('Ms01')
  const [mistake, setMistake] = useState('none')
  const network = useAction()
  const rack = useAction()
  const seed = useAction()
  const onNetwork = (f: LabFault) => network.run(() => labFault(f))
  const chosen = picked.filter((p) => truth.vms.some((v) => v.id === p))
  const group = partitionOf(chosen.join(' '), withSwitch)
  const toggle = (id: string, on: boolean) => setPicked(on ? [...chosen, id] : chosen.filter((p) => p !== id))

  return (
    <section className="lab-page page-grid">
      <Card title="Machines" extra={<span className="muted">{truth.vms.length} VMs, seen from outside</span>} className="span-all">
        <div className="table-wrap">
          <table className="grid-table">
            <thead>
              <tr>
                <th aria-label="pick for a partition" title="Pick machines for a partition group" />
                <th>Machine</th>
                <th>Power</th>
                <th>Cables and disks</th>
                <th className="hide-narrow">Clock</th>
                <th className="hide-narrow">Technician</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {truth.vms.map((v) => {
                const open = expanded === v.id
                return (
                  <Fragment key={v.id}>
                    <tr className={open ? 'selected' : undefined}>
                      <td>
                        <input type="checkbox" checked={chosen.includes(v.id)} onChange={(e) => toggle(v.id, e.target.checked)} aria-label={`pick ${v.id} for a partition`} />
                      </td>
                      <td>
                        <button className="link expander" aria-expanded={open} onClick={() => setExpanded(open ? null : v.id)}>
                          {open ? '▾' : '▸'} <strong>{v.id}</strong>
                        </button>
                        <div className="cell-sub">
                          {v.profile}
                          {v.holds_vip && ' · holds the VIP'}
                          {hosts[v.id] && (
                            <>
                              {' · '}
                              <button className="link" onClick={() => nav.host(v.id)}>
                                host {hosts[v.id].lifecycle}
                              </button>
                            </>
                          )}
                        </div>
                      </td>
                      <td>{powerPill(v)}</td>
                      <td>
                        {v.unplugged.length === 0 && v.failed_disks.length === 0 ? (
                          <span className="muted">all in</span>
                        ) : (
                          <span className="bad">
                            {v.unplugged.length > 0 && `unplugged ${v.unplugged.join(', ')}`}
                            {v.unplugged.length > 0 && v.failed_disks.length > 0 && '; '}
                            {v.failed_disks.length > 0 && `disk pulled ${v.failed_disks.join(', ')}`}
                          </span>
                        )}
                      </td>
                      <td className="num muted hide-narrow">{v.clock_offset_ms === null ? '–' : `${v.clock_offset_ms >= 0 ? '+' : ''}${v.clock_offset_ms} ms`}</td>
                      <td className={`hide-narrow ${v.installer?.includes('failed') ? 'bad' : 'muted'}`} title="what the technician last did to install homelab on it">
                        {v.installer ?? '–'}
                      </td>
                      <td className="row-actions">
                        <PowerActions v={v} compact />
                      </td>
                    </tr>
                    {open && (
                      <tr className="expanded">
                        <td colSpan={7}>
                          <MachineControls vm={v.id} showTruth={false} />
                        </td>
                      </tr>
                    )}
                  </Fragment>
                )
              })}
              {truth.vms.length === 0 && (
                <tr>
                  <td colSpan={7} className="muted">
                    No lab machines.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </Card>

      <Card title="Partitions" extra={<span className="muted">{truth.partitions.length} active</span>}>
        <p className="muted">Machines in a group reach each other and nothing else. Pick machines in the table above.</p>
        <div className="actions">
          <span>{chosen.length === 0 ? <span className="muted">none picked</span> : chosen.join(', ')}</span>
          <label className="inline">
            with{' '}
            <select value={withSwitch} onChange={(e) => setWithSwitch(e.target.value)} aria-label="switch in the partition">
              <option value="">no switch</option>
              {truth.switches.map((s) => (
                <option key={s.id}>{s.id}</option>
              ))}
            </select>
          </label>
        </div>
        <div className="actions">
          <button className="primary" disabled={group === null || network.busy} onClick={() => group && onNetwork(group)}>
            Partition group
          </button>
          <button disabled={network.busy || truth.partitions.length === 0} onClick={() => onNetwork('Heal')}>
            Heal partitions
          </button>
        </div>
        {truth.partitions.length > 0 && (
          <ul className="plain">
            {truth.partitions.map((g, n) => (
              <li key={n}>
                <span className="pill warn">group {n + 1}</span> {g.map(memberLabel).join(', ')}
              </li>
            ))}
          </ul>
        )}
        {network.error && <div className="bad">{network.error}</div>}
      </Card>

      <Card title="Switches">
        <table className="grid-table compact">
          <tbody>
            {truth.switches.map((s) => (
              <tr key={s.id}>
                <td>
                  <button className="link" onClick={() => nav.switch(s.id)}>
                    <strong>{s.id}</strong>
                  </button>
                </td>
                <td>
                  <span className={`pill ${s.up ? 'ok' : 'bad'}`}>{s.up ? 'up' : 'dead'}</span>
                </td>
                <td className="row-actions">
                  {s.up ? (
                    <button className="small" disabled={network.busy} onClick={() => onNetwork({ KillSwitch: { switch: s.id } })}>
                      Kill
                    </button>
                  ) : (
                    <button className="small" disabled={network.busy} onClick={() => onNetwork({ RestoreSwitch: { switch: s.id } })}>
                      Restore
                    </button>
                  )}
                  <button className="small" disabled={network.busy} onClick={() => onNetwork({ Partition: { members: [{ Switch: s.id }] } })} title="Cut the switch off from everything">
                    Partition
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </Card>

      <Card title="Rack a machine">
        <p className="muted">Cable a new VM, have its switch ports configured, install Debian, and have the technician run the join command over SSH with a fresh token.</p>
        <div className="form-grid wide">
          <label>Hardware</label>
          <select value={profile} onChange={(e) => setProfile(e.target.value as ProfileId)} aria-label="profile">
            {PROFILES.map((p) => (
              <option key={p}>{p}</option>
            ))}
          </select>
          <label>Switch ports</label>
          <select value={mistake} onChange={(e) => setMistake(e.target.value)} aria-label="switch port mistake">
            {MISTAKES.map((m) => (
              <option key={m.value} value={m.value}>
                {m.label}
              </option>
            ))}
          </select>
        </div>
        <div className="actions">
          <button className="primary" disabled={rack.busy} onClick={() => rack.run(() => labFault({ AddVm: { profile, mistake: mistakeOf(mistake) } }))}>
            Add VM
          </button>
          {rack.error && <span className="bad">{rack.error}</span>}
        </div>
      </Card>

      <Card title="Demo workloads">
        <p className="muted">
          PostgreSQL with pgbench load and a writer on Longhorn volumes, a web app with clients, and a transcode-style CPU burner, in the <code>demo</code> namespace.
        </p>
        <div className="actions">
          <button disabled={seed.busy} onClick={() => seed.run(() => seedWorkloads())} title="kubectl apply deploy/demo-workloads.yaml">
            Seed workloads
          </button>
          {seed.error && <span className="bad">{seed.error}</span>}
        </div>
      </Card>
    </section>
  )
}
