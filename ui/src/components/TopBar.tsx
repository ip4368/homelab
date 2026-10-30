import { useMemo, useState } from 'react'
import { op } from '../api'
import type { CpInfo } from '../gen/CpInfo'
import type { CpMode } from '../gen/CpMode'
import type { HostObj } from '../gen/HostObj'
import { formatClock } from '../clock'
import { useStore } from '../store'
import { Charts } from './Charts'
import { useAction } from './useAction'

/** A voter is live when its host is in the view and not NotReady. */
export function quorumOf(cp: CpInfo, hosts: Record<string, HostObj>): { live: number; voters: number; ok: boolean } {
  const voters = cp.voters.length
  const live = cp.voters.filter((id) => hosts[id] && hosts[id].lifecycle !== 'NotReady').length
  return { live, voters, ok: voters > 0 && live >= Math.floor(voters / 2) + 1 }
}

/** What a voter-replacement mode does, for the toggle's tooltip. */
export function replacementModeBlurb(mode: CpMode): string {
  return mode === 'Auto'
    ? 'Auto: replaces a voter down 10 minutes without asking'
    : 'Suggest: raises an alert naming a spare once a voter has been down 10 minutes; you replace it'
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
  const clock = useStore((s) => s.clock)
  const connection = useStore((s) => s.connection)
  const [monitoring, setMonitoring] = useState(false)
  const { error: modeError, busy: modeBusy, run: runMode } = useAction()

  const counts = useMemo(() => {
    const c = { Ready: 0, NotReady: 0, Pending: 0 }
    for (const h of Object.values(hosts)) {
      if (h.lifecycle === 'Ready' || h.lifecycle === 'NotReady' || h.lifecycle === 'Pending') c[h.lifecycle]++
    }
    return c
  }, [hosts])
  const activeAlerts = useMemo(() => Object.values(alerts).filter((a) => a.resolved_at === null).length, [alerts])
  const quorum = quorumOf(cp, hosts)

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
        <label className="inline" title={replacementModeBlurb(cp.mode)}>
          Replacement{' '}
          <select
            value={cp.mode}
            disabled={modeBusy}
            aria-label="voter replacement mode"
            onChange={(e) => runMode(() => op({ SetCpMode: { mode: e.target.value as CpMode } }))}
          >
            <option value="Suggest">Suggest</option>
            <option value="Auto">Auto</option>
          </select>
        </label>
        {modeError && <span className="bad">{modeError}</span>}
        <div className="spacer" />
        <button className={monitoring ? 'active' : ''} aria-pressed={monitoring} onClick={() => setMonitoring(!monitoring)}>
          Monitoring
        </button>
        <label className="inline truth-toggle" title="Overlay what the lab sees from outside the machines but the control plane does not know">
          <input type="checkbox" checked={showTruth} onChange={(e) => onShowTruth(e.target.checked)} /> Ground truth
        </label>
        {connection === 'connecting' && <span className="muted">connecting…</span>}
        <span className="clock" title="lab host time">
          {clock.now === 0 ? '--:--:--' : formatClock(clock.now)}
        </span>
      </div>
      {monitoring && (
        <div className="monitoring">
          <Charts entity="fleet" title="Fleet: average CPU and RAM, total port throughput" row />
        </div>
      )}
    </header>
  )
}
