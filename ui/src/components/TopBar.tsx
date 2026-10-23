import { useMemo, useState } from 'react'
import { setSpeed } from '../api'
import type { CpInfo } from '../gen/CpInfo'
import type { HostObj } from '../gen/HostObj'
import { useStore } from '../store'

const SPEEDS = [
  { speed: 0, label: 'Pause' },
  { speed: 1, label: '1×' },
  { speed: 5, label: '5×' },
  { speed: 20, label: '20×' },
  { speed: 60, label: '60×' },
]

/** Sim milliseconds as hh:mm:ss; hours keep counting past 24. */
export function formatClock(ms: number): string {
  const total = Math.floor(ms / 1000)
  const pad = (n: number) => String(n).padStart(2, '0')
  return `${pad(Math.floor(total / 3600))}:${pad(Math.floor(total / 60) % 60)}:${pad(total % 60)}`
}

/** A voter is live when its host is in the view and not NotReady. */
export function quorumOf(cp: CpInfo, hosts: Record<string, HostObj>): { live: number; voters: number; ok: boolean } {
  const voters = cp.voters.length
  const live = cp.voters.filter((id) => hosts[id] && hosts[id].lifecycle !== 'NotReady').length
  return { live, voters, ok: voters > 0 && live >= Math.floor(voters / 2) + 1 }
}

function Stat({ label, value, tone }: { label: string; value: string | number; tone?: 'ok' | 'warn' | 'bad' }) {
  return (
    <div className="stat">
      <span className="stat-label">{label}</span>
      <span className={`stat-value ${tone ?? ''}`}>{value}</span>
    </div>
  )
}

export function TopBar({ showTruth, onShowTruth }: { showTruth: boolean; onShowTruth(on: boolean): void }) {
  const hosts = useStore((s) => s.view.hosts)
  const alerts = useStore((s) => s.view.alerts)
  const cp = useStore((s) => s.view.cp)
  const leader = useStore((s) => s.view.leader)
  const term = useStore((s) => s.view.term)
  const sim = useStore((s) => s.sim)
  const connection = useStore((s) => s.connection)
  const [error, setError] = useState<string | null>(null)

  const counts = useMemo(() => {
    const c = { Ready: 0, NotReady: 0, Pending: 0 }
    for (const h of Object.values(hosts)) {
      if (h.lifecycle === 'Ready' || h.lifecycle === 'NotReady' || h.lifecycle === 'Pending') c[h.lifecycle]++
    }
    return c
  }, [hosts])
  const activeAlerts = useMemo(() => Object.values(alerts).filter((a) => a.resolved_at === null).length, [alerts])
  const quorum = quorumOf(cp, hosts)
  const activeSpeed = sim.paused ? 0 : sim.speed

  const choose = (speed: number) => {
    setError(null)
    setSpeed(speed).catch((e: unknown) => setError(String(e)))
  }

  return (
    <header className="topbar">
      {connection === 'cp-unavailable' && (
        <div className="banner" role="alert">
          control plane unavailable — reconnecting
        </div>
      )}
      <div className="topbar-row">
        <Stat label="Ready" value={counts.Ready} tone="ok" />
        <Stat label="NotReady" value={counts.NotReady} tone={counts.NotReady > 0 ? 'bad' : undefined} />
        <Stat label="Pending" value={counts.Pending} tone={counts.Pending > 0 ? 'warn' : undefined} />
        <Stat label="Leader" value={leader ? `${leader} (term ${term})` : 'none'} tone={leader ? undefined : 'bad'} />
        <Stat
          label="Quorum"
          value={`${quorum.live}/${quorum.voters}`}
          tone={quorum.voters === 0 ? undefined : quorum.ok ? 'ok' : 'bad'}
        />
        <Stat label="Alerts" value={activeAlerts} tone={activeAlerts > 0 ? 'warn' : undefined} />
        <div className="spacer" />
        <label className="inline truth-toggle" title="Overlay what the simulator knows but the control plane does not">
          <input type="checkbox" checked={showTruth} onChange={(e) => onShowTruth(e.target.checked)} /> Ground truth
        </label>
        {connection === 'connecting' && <span className="muted">connecting…</span>}
        {error && <span className="bad">{error}</span>}
        <span className="clock" title="simulated time">
          {formatClock(sim.now)}
        </span>
        <div className="speeds" role="group" aria-label="simulation speed">
          {SPEEDS.map(({ speed, label }) => (
            <button key={speed} className={speed === activeSpeed ? 'active' : ''} onClick={() => choose(speed)}>
              {label}
            </button>
          ))}
        </div>
      </div>
    </header>
  )
}
