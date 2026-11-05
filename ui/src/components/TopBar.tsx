import { useMemo } from 'react'
import type { CpInfo } from '../gen/CpInfo'
import type { HostObj } from '../gen/HostObj'
import { nav } from '../nav'
import { PAGE_LABELS, type Page } from '../route'
import { useStore } from '../store'
import { Icon } from './Icons'

/** A voter is live when its host is in the view and not NotReady. */
export function quorumOf(cp: CpInfo, hosts: Record<string, HostObj>): { live: number; voters: number; ok: boolean } {
  const voters = cp.voters.length
  const live = cp.voters.filter((id) => hosts[id] && hosts[id].lifecycle !== 'NotReady').length
  return { live, voters, ok: voters > 0 && live >= Math.floor(voters / 2) + 1 }
}

/** One fact about the cluster in the header, a button where it leads somewhere. */
function Chip({ label, value, tone, title, onClick }: { label: string; value: string | number; tone?: 'ok' | 'warn' | 'bad'; title?: string; onClick?(): void }) {
  const body = (
    <>
      <span className={`dot ${tone ?? 'muted'}`} aria-hidden />
      <span className="chip-label">{label}</span>
      <span className="chip-value">{value}</span>
    </>
  )
  return onClick ? (
    <button className="chip" title={title} onClick={onClick}>
      {body}
    </button>
  ) : (
    <span className="chip" title={title}>
      {body}
    </span>
  )
}

/**
 * The header over every page: where you are, the cluster's health in a few
 * chips (each opens what it counts), and the activity panel's toggle.
 */
export function TopBar({ page, onMenu, activityOpen, onActivity }: { page: Page; onMenu(): void; activityOpen: boolean; onActivity(): void }) {
  const hosts = useStore((s) => s.view.hosts)
  const alerts = useStore((s) => s.view.alerts)
  const cp = useStore((s) => s.view.cp)
  const leader = useStore((s) => s.view.leader)
  const term = useStore((s) => s.view.term)
  const workloads = useStore((s) => s.kube.workloads)
  const kubeAt = useStore((s) => s.kube.at)

  const counts = useMemo(() => {
    const all = Object.values(hosts).filter((h) => h.lifecycle !== 'Removed')
    return {
      total: all.length,
      ready: all.filter((h) => h.lifecycle === 'Ready').length,
      down: all.filter((h) => h.lifecycle === 'NotReady').length,
      pending: all.filter((h) => h.lifecycle === 'Pending').length,
    }
  }, [hosts])
  const open = useMemo(() => Object.values(alerts).filter((a) => a.resolved_at === null), [alerts])
  const critical = open.some((a) => a.severity === 'Critical')
  const healthy = workloads.filter((w) => w.health.state === 'Healthy' || w.health.state === 'Idle').length
  const down = workloads.some((w) => w.health.state === 'Down')
  const quorum = quorumOf(cp, hosts)

  return (
    <header className="topbar">
      <button className="icon-button nav-toggle" onClick={onMenu} aria-label="navigation">
        <Icon name="menu" />
      </button>
      <h1>{PAGE_LABELS[page]}</h1>
      <div className="chips">
        <Chip
          label="Hosts"
          value={`${counts.ready}/${counts.total} ready`}
          tone={counts.down > 0 ? 'bad' : counts.pending > 0 ? 'warn' : counts.total > 0 ? 'ok' : undefined}
          title={`${counts.down} NotReady, ${counts.pending} waiting to join`}
          onClick={() => nav.page('hosts')}
        />
        {kubeAt > 0 && (
          <Chip
            label="Workloads"
            value={`${healthy}/${workloads.length} healthy`}
            tone={down ? 'bad' : healthy < workloads.length ? 'warn' : 'ok'}
            onClick={() => nav.page('workloads')}
          />
        )}
        <Chip
          label="Quorum"
          value={`${quorum.live}/${quorum.voters}`}
          tone={quorum.voters === 0 ? undefined : quorum.ok ? 'ok' : 'bad'}
          title="Control-plane voters up, of all voters"
        />
        <Chip label="Leader" value={leader ? `${leader} · term ${term}` : 'none'} tone={leader ? 'ok' : 'bad'} title="Holds the VIP and answers the API" onClick={leader ? () => nav.host(leader) : undefined} />
      </div>
      <div className="spacer" />
      <button
        className={`icon-button activity-toggle${activityOpen ? ' pressed' : ''}`}
        onClick={onActivity}
        aria-pressed={activityOpen}
        aria-label={`activity: ${open.length} open alerts`}
        title="Alerts, events and pending joins"
      >
        <Icon name="bell" size={18} />
        {open.length > 0 && <span className={`badge-count ${critical ? 'bad' : 'warn'}`}>{open.length}</span>}
      </button>
    </header>
  )
}
