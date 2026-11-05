import { useMemo } from 'react'
import { formatClock } from '../clock'
import { nav } from '../nav'
import { PAGE_LABELS, PAGES, type Page } from '../route'
import { useStore } from '../store'
import { Icon } from './Icons'

/** What each page's entry counts: problems there, coloured by the worst. */
function useBadges(): Partial<Record<Page, { n: number; tone: 'bad' | 'warn' }>> {
  const hosts = useStore((s) => s.view.hosts)
  const alerts = useStore((s) => s.view.alerts)
  const workloads = useStore((s) => s.kube.workloads)
  const truth = useStore((s) => s.truth)
  return useMemo(() => {
    const all = Object.values(hosts)
    const down = all.filter((h) => h.lifecycle === 'NotReady').length
    const pending = all.filter((h) => h.lifecycle === 'Pending').length
    const wDown = workloads.filter((w) => w.health.state === 'Down').length
    const wWarn = workloads.filter((w) => w.health.state === 'Degraded' || w.health.state === 'RollingOut').length
    const open = Object.values(alerts).filter((a) => a.resolved_at === null)
    const switchAlerts = open.filter((a) => a.entity.kind === 'Switch').length
    const lab = truth.partitions.length + truth.vms.filter((v) => v.installed && v.power !== 'Running').length + truth.switches.filter((s) => !s.up).length
    const b: Partial<Record<Page, { n: number; tone: 'bad' | 'warn' }>> = {}
    if (open.length) b.overview = { n: open.length, tone: open.some((a) => a.severity === 'Critical') ? 'bad' : 'warn' }
    if (down + pending) b.hosts = { n: down + pending, tone: down ? 'bad' : 'warn' }
    if (wDown + wWarn) b.workloads = { n: wDown + wWarn, tone: wDown ? 'bad' : 'warn' }
    if (switchAlerts) b.topology = { n: switchAlerts, tone: 'warn' }
    if (lab) b.lab = { n: lab, tone: 'warn' }
    return b
  }, [hosts, alerts, workloads, truth])
}

const ICONS = { overview: 'overview', hosts: 'hosts', workloads: 'workloads', topology: 'topology', networks: 'networks', lab: 'lab' } as const

/**
 * The navigation down the left: every page with a count of its problems,
 * then the ground-truth overlay, the connection and the lab host's clock.
 * Collapsed, it is a strip of icons.
 */
export function Sidebar({ page, collapsed, onCollapse, showTruth, onShowTruth }: { page: Page; collapsed: boolean; onCollapse(c: boolean): void; showTruth: boolean; onShowTruth(on: boolean): void }) {
  const badges = useBadges()
  const clock = useStore((s) => s.clock)
  const connection = useStore((s) => s.connection)
  const labTitles: Partial<Record<Page, string>> = {
    lab: 'Lab VMs: partitions, machines not running, switches down',
    overview: 'Open alerts',
    hosts: 'Hosts NotReady or waiting to join',
    workloads: 'Workloads down, degraded or rolling out',
    topology: 'Switch alerts',
  }
  return (
    <nav className="sidebar" aria-label="pages">
      <div className="brand">
        <span className="brand-mark" aria-hidden>
          ◆
        </span>
        <span className="brand-name">homelab</span>
      </div>
      <ul>
        {PAGES.map((p) => {
          const badge = badges[p]
          return (
            <li key={p}>
              <a
                href={`#/${p}`}
                className={p === page ? 'active' : undefined}
                aria-current={p === page ? 'page' : undefined}
                title={collapsed ? PAGE_LABELS[p] : undefined}
                onClick={(e) => {
                  e.preventDefault()
                  nav.page(p)
                }}
              >
                <Icon name={ICONS[p]} size={18} />
                <span className="nav-label">{PAGE_LABELS[p]}</span>
                {badge && (
                  <span className={`badge-count ${badge.tone}`} title={labTitles[p]}>
                    {badge.n}
                  </span>
                )}
              </a>
            </li>
          )
        })}
      </ul>
      <div className="spacer" />
      <div className="sidebar-foot">
        <label className="truth-switch" title="Overlay what the lab sees from outside the machines but the control plane does not know">
          <input type="checkbox" checked={showTruth} onChange={(e) => onShowTruth(e.target.checked)} />
          <span className="nav-label">Ground truth</span>
        </label>
        <div className="conn" title={connection === 'live' ? 'Connected to the control plane' : connection === 'connecting' ? 'Connecting' : 'Control plane unavailable'}>
          <span className={`dot ${connection === 'live' ? 'ok' : connection === 'connecting' ? 'warn' : 'bad'}`} aria-hidden />
          <span className="clock" title="lab host time">
            {clock.now === 0 ? '--:--:--' : formatClock(clock.now)}
          </span>
        </div>
        <button className="icon-button collapse" onClick={() => onCollapse(!collapsed)} aria-label={collapsed ? 'expand navigation' : 'collapse navigation'} title={collapsed ? 'Expand' : 'Collapse'}>
          <Icon name={collapsed ? 'chevronRight' : 'chevronLeft'} />
        </button>
      </div>
    </nav>
  )
}
