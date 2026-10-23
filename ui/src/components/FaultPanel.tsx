import { useMemo, useState } from 'react'
import { fault, scenario } from '../api'
import type { Fault } from '../gen/Fault'
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

export function FaultPanel({ selected }: { selected: string | null }) {
  const [open, setOpen] = useState(true)
  const paused = useStore((s) => s.sim.paused)
  const truth = useStore((s) => s.truth)
  const [switchId, setSwitchId] = useState('')
  const [profile, setProfile] = useState<ProfileId>('Ms01')
  const [count, setCount] = useState(1)
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

  // Switch ids are known from the ground truth only while a VIP table lists them; otherwise type one.
  const switchIds = Object.keys(truth.vip_owner_by_switch)
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
              <button
                disabled={switchId.trim() === '' || networkAction.busy}
                onClick={() => injectNetwork({ Partition: { nodes: [{ Switch: switchId.trim() }] } })}
              >
                Partition switch
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
              <button
                disabled={paused || scenarioAction.busy}
                onClick={() => scenarioAction.run(() => scenario({ AddHosts: { profile, count, misconfigure: null } }))}
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
