import { useMemo, useState } from 'react'
import { fault, scenario } from '../api'
import type { Fault } from '../gen/Fault'
import type { Misconfig } from '../gen/Misconfig'
import type { PartitionMember } from '../gen/PartitionMember'
import type { ProfileId } from '../gen/ProfileId'
import { useStore } from '../store'
import { useAction } from './useAction'

const ROUTER = '__router__'

/** Iperf form: a load from one host to another host or the router, over a network. */
function IperfForm({ selected }: { selected: string | null }) {
  const paused = useStore((s) => s.sim.paused)
  const hosts = useStore((s) => s.view.hosts)
  const networks = useStore((s) => s.view.networks)
  const [from, setFrom] = useState('')
  const [to, setTo] = useState(ROUTER)
  const [network, setNetwork] = useState('')
  const [mbps, setMbps] = useState(1000)
  const [streams, setStreams] = useState(1)
  const [duration, setDuration] = useState(30)
  const { error, busy, run } = useAction()
  const hostIds = useMemo(() => Object.keys(hosts).sort(), [hosts])
  const networkIds = useMemo(() => Object.keys(networks).sort(), [networks])
  // The selected host is the default source; without one, the first host.
  const source = from || selected || hostIds[0] || ''
  const net = network || (networkIds.includes('storage') ? 'storage' : (networkIds[0] ?? ''))
  const valid = source !== '' && net !== '' && mbps > 0 && streams >= 1 && duration >= 1 && to !== source
  const start = () =>
    run(() => scenario({ Iperf: { from: source, to: to === ROUTER ? 'Router' : { Host: to }, network: net, mbps, duration_s: duration, streams } }))
  const num = (set: (n: number) => void) => (e: { target: { value: string } }) => set(Math.max(0, Math.floor(Number(e.target.value)) || 0))
  return (
    <div className="fault-group">
      <h4>Iperf</h4>
      <div className="actions">
        <label className="inline">
          from{' '}
          <select value={source} onChange={(e) => setFrom(e.target.value)} aria-label="iperf from">
            {hostIds.map((h) => (
              <option key={h}>{h}</option>
            ))}
          </select>
        </label>
        <label className="inline">
          to{' '}
          <select value={to} onChange={(e) => setTo(e.target.value)} aria-label="iperf to">
            <option value={ROUTER}>Router</option>
            {hostIds.map((h) => (
              <option key={h}>{h}</option>
            ))}
          </select>
        </label>
        <label className="inline">
          on{' '}
          <select value={net} onChange={(e) => setNetwork(e.target.value)} aria-label="iperf network">
            {networkIds.map((n) => (
              <option key={n}>{n}</option>
            ))}
          </select>
        </label>
        <label className="inline">
          <input type="number" min={1} value={mbps} onChange={num(setMbps)} style={{ width: 70 }} aria-label="mbps per stream" /> Mb/s per stream
        </label>
        <label className="inline">
          <input type="number" min={1} value={streams} onChange={num(setStreams)} style={{ width: 48 }} aria-label="streams" /> streams
        </label>
        <label className="inline">
          <input type="number" min={1} value={duration} onChange={num(setDuration)} style={{ width: 56 }} aria-label="duration seconds" /> s
        </label>
        <button disabled={paused || busy || !valid} onClick={start}>
          Start iperf
        </button>
      </div>
      {paused && <div className="muted">Iperf needs the simulation running.</div>}
      {error && <div className="bad">{error}</div>}
    </div>
  )
}

const PROFILES: ProfileId[] = ['Ms01', 'MiniPc', 'Pi5', 'Rk3588']

/** The switch-port mistakes "Add hosts" can make on the new hosts' ports. */
export const MISCONFIGS = [
  { value: 'none', label: 'correctly cabled', misconfig: null },
  { value: 'MissingVlan20', label: 'VLAN 20 missing', misconfig: { MissingVlan: 20 } },
  { value: 'MtuTooSmall', label: 'MTU too small', misconfig: 'MtuTooSmall' },
  { value: 'BondNoLacp', label: 'bond without LACP', misconfig: 'BondNoLacp' },
] as const satisfies readonly { value: string; label: string; misconfig: Misconfig | null }[]

export function misconfigOf(value: string): Misconfig | null {
  return MISCONFIGS.find((m) => m.value === value)?.misconfig ?? null
}

/** One partition group: the switch (if any) and the hosts listed, separated by commas or spaces. */
export function partitionOf(hosts: string, sw: string): Fault | null {
  const ids = [...new Set(hosts.split(/[\s,]+/).filter((h) => h !== ''))]
  const nodes: PartitionMember[] = [...(sw.trim() ? [{ Switch: sw.trim() }] : []), ...ids.map((h) => ({ Host: h }))]
  return nodes.length > 0 ? { Partition: { nodes } } : null
}

export function FaultPanel({ selected }: { selected: string | null }) {
  const [open, setOpen] = useState(true)
  const paused = useStore((s) => s.sim.paused)
  const truth = useStore((s) => s.truth)
  const [switchId, setSwitchId] = useState('')
  const [profile, setProfile] = useState<ProfileId>('Ms01')
  const [count, setCount] = useState(1)
  const [misconfig, setMisconfig] = useState('none')
  const [mbps, setMbps] = useState(100)
  const [hostSet, setHostSet] = useState('')
  const [withSwitch, setWithSwitch] = useState(false)
  const [clientPort, setClientPort] = useState('')
  const knownSwitches = useStore((s) => s.view.switches)
  const hostAction = useAction()
  const hostNics = useStore((s) => (selected ? s.view.hosts[selected]?.inventory.nics : undefined))
  const hostDisks = useStore((s) => (selected ? s.view.hosts[selected]?.inventory.disks : undefined))
  const [disk, setDisk] = useState('')
  const workingDisks = hostDisks?.filter((d) => !d.failed).map((d) => d.name) ?? []
  const chosenDisk = workingDisks.includes(disk) ? disk : (workingDisks[0] ?? '')
  const [port, setPort] = useState('')
  const nicNames = hostNics?.map((n) => n.name) ?? []
  const chosenPort = nicNames.includes(port) ? port : (nicNames[0] ?? '')
  const networkAction = useAction()
  const scenarioAction = useAction()

  // Switch ids the control plane knows, plus any the ground truth's VIP table lists; or type one.
  const switchIds = [...new Set([...Object.keys(knownSwitches), ...Object.keys(truth.vip_owner_by_switch)])].sort()
  const sw = switchId.trim()
  const group = partitionOf(hostSet, withSwitch ? sw : '')
  const injectHost = (f: Fault) => hostAction.run(() => fault(f))
  const injectNetwork = (f: Fault) => networkAction.run(() => fault(f))
  const needHost = selected === null

  return (
    <section className="faultpanel">
      <button className="link" onClick={() => setOpen(!open)} aria-expanded={open}>
        {open ? '▾' : '▸'} Faults &amp; scenarios
      </button>
      {open && (
        <div className="fault-body">
          <div className="fault-group">
            <h4>Selected host{selected ? `: ${selected}` : ''}</h4>
            <div className="actions">
              <button disabled={needHost || hostAction.busy} onClick={() => injectHost({ KillHost: { host: selected! } })}>
                Kill host
              </button>
              <button disabled={needHost || hostAction.busy} onClick={() => injectHost({ RestoreHost: { host: selected! } })}>
                Restore host
              </button>
              <button disabled={needHost || hostAction.busy} onClick={() => injectHost({ Partition: { nodes: [{ Host: selected! }] } })}>
                Partition host
              </button>
              <select value={chosenPort} onChange={(e) => setPort(e.target.value)} disabled={needHost} aria-label="port">
                {hostNics?.map((n) => (
                  <option key={n.name} value={n.name}>
                    {n.name}
                    {n.link_up ? '' : ' (down)'}
                  </option>
                ))}
              </select>
              <button disabled={needHost || chosenPort === '' || hostAction.busy} onClick={() => injectHost({ UnplugPort: { host: selected!, port: chosenPort } })}>
                Unplug port
              </button>
              <button disabled={needHost || chosenPort === '' || hostAction.busy} onClick={() => injectHost({ ReplugPort: { host: selected!, port: chosenPort } })}>
                Replug port
              </button>
              <select value={chosenDisk} onChange={(e) => setDisk(e.target.value)} disabled={needHost} aria-label="disk">
                {hostDisks?.map((d) => (
                  <option key={d.name} value={d.name} disabled={d.failed}>
                    {d.name}
                    {d.failed ? ' (failed)' : ''}
                  </option>
                ))}
              </select>
              <button disabled={needHost || chosenDisk === '' || hostAction.busy} onClick={() => injectHost({ FailDisk: { host: selected!, disk: chosenDisk } })}>
                Fail disk
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
                disabled={needHost || chosenPort === '' || hostAction.busy}
                onClick={() => injectHost({ DegradePort: { host: selected!, port: chosenPort, mbps } })}
                title="The port negotiates at most this speed (e.g. a bad cable); restore by degrading to its full speed"
              >
                Degrade port
              </button>
            </div>
            {hostAction.error && <div className="bad">{hostAction.error}</div>}
          </div>
          <div className="fault-group">
            <h4>Network</h4>
            <div className="actions">
              <input
                list="switch-ids"
                placeholder="switch id"
                value={switchId}
                onChange={(e) => setSwitchId(e.target.value)}
                aria-label="switch id"
              />
              <datalist id="switch-ids">
                {switchIds.map((s) => (
                  <option key={s} value={s} />
                ))}
              </datalist>
              <button disabled={sw === '' || networkAction.busy} onClick={() => injectNetwork({ KillSwitch: { switch: sw } })}>
                Kill switch
              </button>
              <button disabled={sw === '' || networkAction.busy} onClick={() => injectNetwork({ RestoreSwitch: { switch: sw } })}>
                Restore switch
              </button>
              <button disabled={sw === '' || networkAction.busy} onClick={() => injectNetwork({ Partition: { nodes: [{ Switch: sw }] } })}>
                Partition switch
              </button>
              <input placeholder="switch port" value={clientPort} onChange={(e) => setClientPort(e.target.value)} style={{ width: 80 }} aria-label="client switch port" />
              <button
                disabled={sw === '' || clientPort.trim() === '' || networkAction.busy}
                onClick={() => injectNetwork({ MoveClient: { switch: sw, port: clientPort.trim() } })}
                title="Re-cable the UI client's attachment point to this switch port"
              >
                Move client
              </button>
            </div>
            <div className="actions">
              <input
                placeholder="hosts, e.g. pi5-01 pi5-02"
                value={hostSet}
                onChange={(e) => setHostSet(e.target.value)}
                style={{ width: 200 }}
                aria-label="partition host set"
              />
              <button disabled={needHost} onClick={() => setHostSet(hostSet.trim() ? `${hostSet.trim()} ${selected}` : selected!)}>
                Add selected
              </button>
              <label className="inline">
                <input type="checkbox" checked={withSwitch} onChange={(e) => setWithSwitch(e.target.checked)} /> with the switch
              </label>
              <button disabled={group === null || networkAction.busy} onClick={() => group && injectNetwork(group)}>
                Partition group
              </button>
              <button disabled={networkAction.busy} onClick={() => injectNetwork('HealPartitions')}>
                Heal partitions
              </button>
              <span className="muted">{truth.partitions.length} active partition group(s)</span>
            </div>
            {networkAction.error && <div className="bad">{networkAction.error}</div>}
          </div>
          <IperfForm selected={selected} />
          <div className="fault-group">
            <h4>Scenarios</h4>
            <div className="actions">
              <select value={profile} onChange={(e) => setProfile(e.target.value as ProfileId)} aria-label="profile">
                {PROFILES.map((p) => (
                  <option key={p}>{p}</option>
                ))}
              </select>
              <input
                type="number"
                min={1}
                value={count}
                onChange={(e) => setCount(Math.max(1, Math.floor(Number(e.target.value)) || 1))}
                aria-label="count"
                style={{ width: 64 }}
              />
              <select value={misconfig} onChange={(e) => setMisconfig(e.target.value)} aria-label="switch misconfiguration">
                {MISCONFIGS.map((m) => (
                  <option key={m.value} value={m.value}>
                    {m.label}
                  </option>
                ))}
              </select>
              <button
                disabled={paused || scenarioAction.busy}
                onClick={() => scenarioAction.run(() => scenario({ AddHosts: { profile, count, misconfigure: misconfigOf(misconfig) } }))}
              >
                Add hosts
              </button>
              <button disabled={paused || scenarioAction.busy} onClick={() => scenarioAction.run(() => scenario('SeedWorkloads'))}>
                Seed workloads
              </button>
              <button disabled={paused || scenarioAction.busy} onClick={() => scenarioAction.run(() => scenario({ preset: 'default' }))}>
                Preset: default
              </button>
              <button disabled={paused || scenarioAction.busy} onClick={() => scenarioAction.run(() => scenario({ preset: 'scale500' }))}>
                Preset: scale500
              </button>
            </div>
            {paused && <div className="muted">Scenarios need the simulation running.</div>}
            {scenarioAction.error && <div className="bad">{scenarioAction.error}</div>}
          </div>
        </div>
      )}
    </section>
  )
}
