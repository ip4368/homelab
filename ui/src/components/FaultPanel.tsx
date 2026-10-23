import { useState } from 'react'
import { fault, scenario } from '../api'
import type { Fault } from '../gen/Fault'
import type { ProfileId } from '../gen/ProfileId'
import { useStore } from '../store'
import { useAction } from './useAction'

const PROFILES: ProfileId[] = ['Ms01', 'MiniPc', 'Pi5', 'Rk3588']

export function FaultPanel({ selected }: { selected: string | null }) {
  const [open, setOpen] = useState(true)
  const paused = useStore((s) => s.sim.paused)
  const truth = useStore((s) => s.truth)
  const [switchId, setSwitchId] = useState('')
  const [profile, setProfile] = useState<ProfileId>('Ms01')
  const [count, setCount] = useState(1)
  const hostAction = useAction()
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
