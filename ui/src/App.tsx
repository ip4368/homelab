import { useEffect, useState } from 'react'
import { ActivityPanel } from './components/ActivityPanel'
import { Dock } from './components/Dock'
import { HostDetail } from './components/HostDetail'
import { HostsPage } from './components/HostsPage'
import { LabPage } from './components/LabPage'
import { NetworksPage } from './components/NetworksPage'
import { OverviewPage } from './components/OverviewPage'
import { Sidebar } from './components/Sidebar'
import { SwitchDetail } from './components/SwitchDetail'
import { TopBar } from './components/TopBar'
import { Topology } from './components/Topology'
import { WorkloadDetail } from './components/WorkloadDetail'
import { WorkloadsPage } from './components/WorkloadsPage'
import { workloadKey } from './kube'
import { nav, useRoute } from './nav'
import { usePref } from './prefs'
import { useStore } from './store'

/** Under this width the navigation starts as icons. */
const NARROW = 1100

/** Keys typed into a field, or with a dialog open, are not shortcuts. */
function typing(e: KeyboardEvent): boolean {
  const t = e.target as HTMLElement | null
  return !!t && (t.isContentEditable || ['INPUT', 'TEXTAREA', 'SELECT'].includes(t.tagName) || !!t.closest('.xterm, .modal'))
}

/**
 * The page: navigation on the left, a header with the cluster's health, the
 * page's list with the detail pane beside it, the activity panel on the
 * right, and the dock of logs and terminals along the bottom.
 */
export default function App() {
  const route = useRoute()
  const [collapsed, setCollapsed] = usePref('navCollapsed', window.innerWidth < NARROW)
  const [activity, setActivity] = usePref('activityOpen', false)
  const [navOpen, setNavOpen] = useState(false)
  // `?truth` in the URL opens with the overlay on, so a link can show it.
  const [showTruth, setShowTruth] = useState(() => new URLSearchParams(location.search).has('truth'))
  const open = route.open
  const exists = useStore((s) =>
    open === null ? true : open.kind === 'host' ? open.id in s.view.hosts : open.kind === 'switch' ? open.id in s.view.switches : s.kube.at === 0 || s.kube.workloads.some((w) => workloadKey(w) === open.id),
  )
  const synced = useStore((s) => s.view.version > 0)
  // A removed host, switch or workload must not stay open.
  useEffect(() => {
    if (open && synced && !exists) nav.close()
  }, [open, exists, synced])
  useEffect(() => setNavOpen(false), [route.page])

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (typing(e)) return
      if (e.key === 'Escape' && document.querySelector('.menu, .modal-backdrop') === null) {
        if (route.open) nav.close()
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [route.open])

  const page =
    route.page === 'hosts' ? (
      <HostsPage selected={open?.kind === 'host' ? open.id : null} showTruth={showTruth} />
    ) : route.page === 'workloads' ? (
      <WorkloadsPage list={route.list} selected={open?.kind === 'workload' ? open.id : null} />
    ) : route.page === 'topology' ? (
      <Topology
        selectedSwitch={open?.kind === 'switch' ? open.id : null}
        selectedHost={open?.kind === 'host' ? open.id : null}
        onSelectSwitch={(id) => nav.switch(id)}
        onSelectHost={(id) => nav.host(id)}
      />
    ) : route.page === 'networks' ? (
      <NetworksPage list={route.list} />
    ) : route.page === 'lab' ? (
      <LabPage />
    ) : (
      <OverviewPage />
    )

  const pane =
    open?.kind === 'host' ? (
      <HostDetail key={open.id} id={open.id} tab={route.tab} onTab={nav.tab} onClose={nav.close} showTruth={showTruth} />
    ) : open?.kind === 'workload' ? (
      <WorkloadDetail key={open.id} id={open.id} tab={route.tab} onTab={nav.tab} onClose={nav.close} />
    ) : open?.kind === 'switch' ? (
      <SwitchDetail key={open.id} id={open.id} tab={route.tab} onTab={nav.tab} onClose={nav.close} />
    ) : null

  return (
    <div className={`app${collapsed ? ' nav-collapsed' : ''}${navOpen ? ' nav-open' : ''}${activity ? ' activity-open' : ''}`}>
      <Banner />
      <Sidebar page={route.page} collapsed={collapsed} onCollapse={setCollapsed} showTruth={showTruth} onShowTruth={setShowTruth} />
      {navOpen && <div className="scrim" onClick={() => setNavOpen(false)} />}
      <div className="main">
        <TopBar page={route.page} onMenu={() => setNavOpen(!navOpen)} activityOpen={activity} onActivity={() => setActivity(!activity)} />
        <div className={`content${pane ? ' with-pane' : ''}`}>
          <main className={`page page-${route.page}`}>{page}</main>
          {pane}
        </div>
      </div>
      {activity && <ActivityPanel showTruth={showTruth} onClose={() => setActivity(false)} />}
      <Dock />
    </div>
  )
}

function Banner() {
  const connection = useStore((s) => s.connection)
  if (connection !== 'cp-unavailable') return null
  return (
    <div className="banner" role="alert">
      control plane unavailable — reconnecting
    </div>
  )
}
