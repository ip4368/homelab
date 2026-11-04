import { useEffect, useState } from 'react'
import { LabPanel } from './components/LabPanel'
import { FleetGrid } from './components/FleetGrid'
import { HostDrawer } from './components/HostDrawer'
import { KubeWorkloads } from './components/KubeWorkloads'
import { NetworksPage } from './components/NetworksPage'
import { Overview, type OverviewView } from './components/Overview'
import { RightRail, type RightRailTab } from './components/RightRail'
import { SwitchDrawer } from './components/SwitchDrawer'
import { WorkloadDrawer } from './components/WorkloadDrawer'
import { workloadKey } from './kube'
import { TopBar } from './components/TopBar'
import { Topology } from './components/Topology'
import { useStore } from './store'

const VIEWS = ['overview', 'fleet', 'topology', 'networks', 'workloads'] as const
type ViewName = (typeof VIEWS)[number]
const LABELS: Record<ViewName, string> = { overview: 'Overview', fleet: 'Fleet', topology: 'Topology', networks: 'Networks', workloads: 'Workloads' }
const SHORT: Record<ViewName, string> = { overview: '◉', fleet: '▦', topology: '⌁', networks: '⇄', workloads: '▤' }

/** The view named by the URL hash (`#topology`), so a link can open it directly. */
function viewFromHash(hash: string): ViewName {
  return VIEWS.find((v) => `#${v}` === hash) ?? 'overview'
}

function storedFlag(key: string, fallback: () => boolean): boolean {
  try {
    const value = window.localStorage.getItem(key)
    return value === null ? fallback() : value === '1'
  } catch {
    return fallback()
  }
}

function storeFlag(key: string, value: boolean) {
  try {
    window.localStorage.setItem(key, value ? '1' : '0')
  } catch {
    // A private window may refuse storage; the preference is only for this session then.
  }
}

export default function App() {
  const [view, setView] = useState<ViewName>(() => viewFromHash(location.hash))
  const [host, setHost] = useState<string | null>(null)
  const [sw, setSwitch] = useState<string | null>(null)
  const [workload, setWorkload] = useState<string | null>(null)
  const [dockOpen, setDockOpenState] = useState(() => storedFlag('homelab:dock-open', () => window.innerWidth >= 1500))
  const [dockTab, setDockTab] = useState<RightRailTab>('Alerts')
  const [labOpen, setLabOpenState] = useState(() => storedFlag('homelab:lab-open', () => false))
  // `?truth` in the URL opens with the overlay on, so a link can show it.
  const [showTruth, setShowTruth] = useState(() => new URLSearchParams(location.search).has('truth'))
  const hostExists = useStore((s) => host !== null && host in s.view.hosts)
  const switchExists = useStore((s) => sw !== null && sw in s.view.switches)
  const workloadExists = useStore((s) => workload !== null && s.kube.workloads.some((w) => workloadKey(w) === workload))
  const alertCount = useStore((s) => Object.values(s.view.alerts).filter((a) => a.resolved_at === null).length)
  const pendingCount = useStore((s) => Object.values(s.view.hosts).filter((h) => h.lifecycle === 'Pending').length)
  const vmCount = useStore((s) => s.truth.vms.length)

  const setDockOpen = (open: boolean) => {
    setDockOpenState(open)
    storeFlag('homelab:dock-open', open)
  }
  const setLabOpen = (open: boolean) => {
    setLabOpenState(open)
    storeFlag('homelab:lab-open', open)
  }

  // A removed host, switch or workload must not stay selected.
  useEffect(() => {
    if (host !== null && !hostExists) setHost(null)
  }, [host, hostExists])
  useEffect(() => {
    if (sw !== null && !switchExists) setSwitch(null)
  }, [sw, switchExists])
  useEffect(() => {
    if (workload !== null && !workloadExists) setWorkload(null)
  }, [workload, workloadExists])
  // The switch drawer belongs to the topology view, the workload drawer to the workloads view.
  useEffect(() => {
    if (view !== 'topology') setSwitch(null)
    if (view !== 'workloads') setWorkload(null)
  }, [view])
  useEffect(() => {
    const onHash = () => setView(viewFromHash(location.hash))
    window.addEventListener('hashchange', onHash)
    return () => window.removeEventListener('hashchange', onHash)
  }, [])

  const go = (next: ViewName) => {
    if (location.hash === `#${next}`) setView(next)
    else location.hash = next
  }

  // One inspector at a time. On ordinary desktop widths it takes the
  // Activity dock's room instead of squeezing the workspace to a sliver.
  const makeRoomForInspector = () => {
    if (window.innerWidth < 1750) setDockOpenState(false)
  }
  const selectHost = (id: string) => {
    makeRoomForInspector()
    setHost(id)
    setSwitch(null)
    setWorkload(null)
  }
  const selectSwitch = (id: string) => {
    makeRoomForInspector()
    setSwitch(id)
    setHost(null)
    setWorkload(null)
  }
  const selectWorkload = (key: string) => {
    makeRoomForInspector()
    setWorkload(key)
    setHost(null)
    setSwitch(null)
  }
  const openOverviewTarget = (next: OverviewView) => go(next)
  const openWorkload = (key: string) => {
    selectWorkload(key)
    go('workloads')
  }
  const openSwitch = (id: string) => {
    selectSwitch(id)
    go('topology')
  }
  const openActivity = (tab: RightRailTab = 'Alerts') => {
    setDockTab(tab)
    setDockOpen(true)
  }

  return (
    <div className="app mission-shell">
      <TopBar showTruth={showTruth} onShowTruth={setShowTruth} />
      <div className={`workspace ${host || workload || sw ? 'has-inspector' : ''}`}>
        <nav className="primary-nav" aria-label="Primary">
          <div className="nav-brand" aria-label="Homelab operations">
            <span className="brand-mark">H</span>
            <span className="brand-copy">
              <strong>Homelab</strong>
              <small>operations</small>
            </span>
          </div>
          <div className="nav-section-label">Workspace</div>
          {VIEWS.map((v) => (
            <button key={v} className={`nav-item ${v === view ? 'active' : ''}`} onClick={() => go(v)} aria-current={v === view ? 'page' : undefined} title={LABELS[v]}>
              <span className="nav-glyph" aria-hidden>{SHORT[v]}</span>
              <span className="nav-label">{LABELS[v]}</span>
              {v === view && <span className="nav-current" aria-hidden />}
            </button>
          ))}
          <div className="nav-spacer" />
          <div className="nav-section-label">Consoles</div>
          <button className={`nav-item ${labOpen ? 'active' : ''}`} onClick={() => setLabOpen(!labOpen)} aria-expanded={labOpen} title="Lab controls">
            <span className="nav-glyph" aria-hidden>⚙</span>
            <span className="nav-label">Lab controls</span>
            {vmCount > 0 && <span className="nav-badge">{vmCount}</span>}
          </button>
          <button className={`nav-item ${dockOpen ? 'active' : ''}`} onClick={() => (dockOpen ? setDockOpen(false) : openActivity())} aria-expanded={dockOpen} title="Activity dock">
            <span className="nav-glyph" aria-hidden>◐</span>
            <span className="nav-label">Activity</span>
            {(alertCount > 0 || pendingCount > 0) && <span className="nav-badge warn">{alertCount + pendingCount}</span>}
          </button>
          <div className="nav-foot muted">Inspectors dock beside the work; nothing important is buried under a page-long scroll.</div>
        </nav>

        <div className="center">
          <div className="grid-wrap">
            <main className="grid-area">
              {view === 'overview' ? (
                <Overview onNavigate={openOverviewTarget} onSelectHost={selectHost} onSelectWorkload={openWorkload} onSelectSwitch={openSwitch} onOpenActivity={() => openActivity('Alerts')} />
              ) : view === 'fleet' ? (
                <FleetGrid selected={host} showTruth={showTruth} onSelect={selectHost} />
              ) : view === 'networks' ? (
                <NetworksPage />
              ) : view === 'workloads' ? (
                <KubeWorkloads onSelectHost={selectHost} onSelectWorkload={selectWorkload} />
              ) : (
                <Topology selectedSwitch={sw} selectedHost={host} onSelectSwitch={selectSwitch} onSelectHost={selectHost} />
              )}
            </main>
            {host && <HostDrawer key={host} id={host} onClose={() => setHost(null)} />}
            {view === 'workloads' && workload && <WorkloadDrawer key={workload} id={workload} onClose={() => setWorkload(null)} onSelectHost={selectHost} />}
            {view === 'topology' && sw && <SwitchDrawer key={sw} id={sw} onClose={() => setSwitch(null)} onSelectHost={selectHost} onSelectSwitch={selectSwitch} />}
          </div>
          {labOpen && (
            <section className="lab-tray" aria-label="Lab controls tray">
              <div className="lab-tray-head">
                <strong>Lab controls</strong>
                <span className="muted">Hardware faults, racking, and demo workloads</span>
                <div className="spacer" />
                <button onClick={() => setLabOpen(false)}>Close tray</button>
              </div>
              <LabPanel selected={host} collapsible={false} />
            </section>
          )}
        </div>

        {dockOpen ? (
          <RightRail onSelect={selectHost} showTruth={showTruth} tab={dockTab} onTabChange={setDockTab} onCollapse={() => setDockOpen(false)} />
        ) : (
          <aside className="dock-strip" aria-label="Collapsed activity dock">
            <button onClick={() => openActivity('Alerts')} title="Open alerts">
              <span>Alerts</span>
              {alertCount > 0 && <b>{alertCount}</b>}
            </button>
            <button onClick={() => openActivity('Events')} title="Open events">
              <span>Events</span>
            </button>
            <button onClick={() => openActivity('Pending joins')} title="Open pending joins">
              <span>Joins</span>
              {pendingCount > 0 && <b>{pendingCount}</b>}
            </button>
          </aside>
        )}
      </div>
    </div>
  )
}
