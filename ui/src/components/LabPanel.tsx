import { useMemo, useState } from 'react'
import { labFault } from '../api'
import type { LabFault } from '../gen/LabFault'
import type { LabMember } from '../gen/LabMember'
import type { Mistake } from '../gen/Mistake'
import type { Power } from '../gen/Power'
import type { ProfileId } from '../gen/ProfileId'
import { useStore } from '../store'
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

export function LabPanel({ selected }: { selected: string | null }) {
  const [open, setOpen] = useState(true)
  const truth = useStore((s) => s.truth)
  // The machine picked here, and the fleet selection it was picked under.
  const [chosen, setChosen] = useState({ vm: '', under: selected })
  const [nic, setNic] = useState('')
  const [mbps, setMbps] = useState(100)
  const [disk, setDisk] = useState('')
  const [switchId, setSwitchId] = useState('')
  const [hostSet, setHostSet] = useState('')
  const [withSwitch, setWithSwitch] = useState(false)
  const [profile, setProfile] = useState<ProfileId>('Ms01')
  const [mistake, setMistake] = useState('none')
  const machine = useAction()
  const network = useAction()
  const rack = useAction()

  const vmIds = useMemo(() => truth.vms.map((v) => v.id), [truth.vms])
  // The one picked here until the fleet selection changes; then the host selected in the fleet, if the lab has it.
  const pickedHere = chosen.under === selected && vmIds.includes(chosen.vm)
  const vm = pickedHere ? chosen.vm : selected && vmIds.includes(selected) ? selected : (vmIds[0] ?? '')
  const vmTruth = truth.vms.find((v) => v.id === vm)
  const nics = useMemo(() => truth.ports.filter((p) => p.host === vm && p.nic !== null).map((p) => p.nic as string), [truth.ports, vm])
  const chosenNic = nics.includes(nic) ? nic : (nics[0] ?? '')
  const disks = vmTruth?.disks ?? []
  const chosenDisk = disks.includes(disk) ? disk : (disks[0] ?? '')
  const diskFailed = vmTruth?.failed_disks.includes(chosenDisk) ?? false
  const switchIds = truth.switches.map((s) => s.id)
  const sw = switchIds.includes(switchId) ? switchId : (switchIds[0] ?? '')
  const group = partitionOf(hostSet, withSwitch ? sw : '')
  const onMachine = (f: LabFault) => machine.run(() => labFault(f))
  const onNetwork = (f: LabFault) => network.run(() => labFault(f))
  const noVm = vm === ''
  const power = vmTruth?.power ?? 'Off'
  const installed = vmTruth?.installed ?? false
  // Degrading reaches into the guest unless it is off: not while frozen or not answering.
  const canDegrade = chosenNic !== '' && (power === 'Running' || power === 'Off')

  return (
    <section className="faultpanel">
      <button className="link" onClick={() => setOpen(!open)} aria-expanded={open}>
        {open ? '▾' : '▸'} Lab
      </button>
      {open && (
        <div className="fault-body">
          <div className="fault-group">
            <h4>
              Machine{' '}
              <select value={vm} onChange={(e) => setChosen({ vm: e.target.value, under: selected })} aria-label="lab machine">
                {vmIds.map((id) => (
                  <option key={id}>{id}</option>
                ))}
              </select>{' '}
              <span className="muted">{vmTruth ? (vmTruth.installed ? POWER_LABEL[power] : 'not installed') : ''}</span>
            </h4>
            <div className="actions">
              <button disabled={noVm || machine.busy || !installed || power !== 'Off'} onClick={() => onMachine({ PowerOn: { vm } })}>
                Power on
              </button>
              <button disabled={noVm || machine.busy || power !== 'Running'} onClick={() => onMachine({ Shutdown: { vm } })} title="Press the power button: a clean shutdown">
                Shut down
              </button>
              <button disabled={noVm || machine.busy || power === 'Off'} onClick={() => onMachine({ PowerOff: { vm } })} title="Pull the plug">
                Power off
              </button>
              <button disabled={noVm || machine.busy || power !== 'Running'} onClick={() => onMachine({ Reset: { vm } })} title="Press reset: an instant reboot">
                Reset
              </button>
              <button disabled={noVm || machine.busy || power !== 'Running'} onClick={() => onMachine({ Freeze: { vm } })} title="Stop its CPUs: it sees no time pass">
                Freeze
              </button>
              <button disabled={noVm || machine.busy || power !== 'Paused'} onClick={() => onMachine({ Thaw: { vm } })}>
                Thaw
              </button>
              <button disabled={noVm || machine.busy} onClick={() => onMachine({ Partition: { members: [{ Host: vm }] } })}>
                Partition
              </button>
            </div>
            <div className="actions">
              <select value={chosenNic} onChange={(e) => setNic(e.target.value)} disabled={noVm} aria-label="nic">
                {nics.map((n) => (
                  <option key={n} value={n}>
                    {n}
                    {vmTruth?.unplugged.includes(n) ? ' (unplugged)' : ''}
                  </option>
                ))}
              </select>
              <button disabled={chosenNic === '' || machine.busy} onClick={() => onMachine({ Unplug: { vm, nic: chosenNic } })}>
                Unplug
              </button>
              <button disabled={chosenNic === '' || machine.busy} onClick={() => onMachine({ Replug: { vm, nic: chosenNic } })}>
                Replug
              </button>
              <label className="inline">
                <input
                  type="number"
                  min={1}
                  value={mbps}
                  onChange={(e) => setMbps(Math.max(1, Math.floor(Number(e.target.value)) || 1))}
                  style={{ width: 70 }}
                  aria-label="degraded port speed"
                />{' '}
                Mb/s
              </label>
              <button
                disabled={!canDegrade || machine.busy}
                onClick={() => onMachine({ Degrade: { vm, nic: chosenNic, mbps } })}
                title="The switch port carries at most this speed (like a bad cable)"
              >
                Degrade
              </button>
              <button disabled={!canDegrade || machine.busy} onClick={() => onMachine({ Degrade: { vm, nic: chosenNic, mbps: null } })}>
                Full speed
              </button>
            </div>
            <div className="actions">
              <select value={chosenDisk} onChange={(e) => setDisk(e.target.value)} disabled={noVm} aria-label="disk">
                {disks.map((d) => (
                  <option key={d} value={d}>
                    {d}
                    {vmTruth?.failed_disks.includes(d) ? ' (pulled)' : ''}
                  </option>
                ))}
              </select>
              <button disabled={chosenDisk === '' || diskFailed || machine.busy} onClick={() => onMachine({ FailDisk: { vm, disk: chosenDisk } })} title="Pull the data disk, as if it died">
                Fail disk
              </button>
              <button disabled={chosenDisk === '' || !diskFailed || machine.busy} onClick={() => onMachine({ RestoreDisk: { vm, disk: chosenDisk } })} title="Put the pulled disk back, with what was on it">
                Restore disk
              </button>
            </div>
            {machine.error && <div className="bad">{machine.error}</div>}
          </div>
          <div className="fault-group">
            <h4>Network</h4>
            <div className="actions">
              <select value={sw} onChange={(e) => setSwitchId(e.target.value)} aria-label="switch">
                {truth.switches.map((s) => (
                  <option key={s.id} value={s.id}>
                    {s.id}
                    {s.up ? '' : ' (dead)'}
                  </option>
                ))}
              </select>
              <button disabled={sw === '' || network.busy} onClick={() => onNetwork({ KillSwitch: { switch: sw } })}>
                Kill switch
              </button>
              <button disabled={sw === '' || network.busy} onClick={() => onNetwork({ RestoreSwitch: { switch: sw } })}>
                Restore switch
              </button>
              <button disabled={sw === '' || network.busy} onClick={() => onNetwork({ Partition: { members: [{ Switch: sw }] } })}>
                Partition switch
              </button>
            </div>
            <div className="actions">
              <input placeholder="machines, e.g. ms01-01 ms01-02" value={hostSet} onChange={(e) => setHostSet(e.target.value)} style={{ width: 200 }} aria-label="partition host set" />
              <button disabled={noVm} onClick={() => setHostSet(hostSet.trim() ? `${hostSet.trim()} ${vm}` : vm)}>
                Add machine
              </button>
              <label className="inline">
                <input type="checkbox" checked={withSwitch} onChange={(e) => setWithSwitch(e.target.checked)} /> with the switch
              </label>
              <button disabled={group === null || network.busy} onClick={() => group && onNetwork(group)}>
                Partition group
              </button>
              <button disabled={network.busy} onClick={() => onNetwork('Heal')}>
                Heal partitions
              </button>
              <span className="muted">{truth.partitions.length} active partition group(s)</span>
            </div>
            {network.error && <div className="bad">{network.error}</div>}
          </div>
          <div className="fault-group">
            <h4>Rack a machine</h4>
            <div className="actions">
              <select value={profile} onChange={(e) => setProfile(e.target.value as ProfileId)} aria-label="profile">
                {PROFILES.map((p) => (
                  <option key={p}>{p}</option>
                ))}
              </select>
              <select value={mistake} onChange={(e) => setMistake(e.target.value)} aria-label="switch port mistake">
                {MISTAKES.map((m) => (
                  <option key={m.value} value={m.value}>
                    {m.label}
                  </option>
                ))}
              </select>
              <button
                disabled={rack.busy}
                onClick={() => rack.run(() => labFault({ AddVm: { profile, mistake: mistakeOf(mistake) } }))}
                title="Cable a new VM, have its switch ports configured, and boot it with a fresh join token"
              >
                Add VM
              </button>
            </div>
            {rack.error && <div className="bad">{rack.error}</div>}
          </div>
          <div className="fault-group">
            <h4>Machines, seen from outside</h4>
            <table className="ports">
              <tbody>
                {truth.vms.map((v) => (
                  <tr key={v.id}>
                    <td>{v.id}</td>
                    <td>{v.installed ? POWER_LABEL[v.power] : 'not installed'}</td>
                    <td>{v.holds_vip ? 'holds the VIP' : ''}</td>
                    <td className="muted">{v.clock_offset_ms === null ? '' : `clock ${v.clock_offset_ms >= 0 ? '+' : ''}${v.clock_offset_ms} ms`}</td>
                    <td className="muted">{v.unplugged.length > 0 ? `unplugged: ${v.unplugged.join(', ')}` : ''}</td>
                    <td className="muted">{v.failed_disks.length > 0 ? `disk pulled: ${v.failed_disks.join(', ')}` : ''}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </section>
  )
}
