import { useEffect, useState } from 'react'
import { FaultPanel } from './components/FaultPanel'
import { FleetGrid } from './components/FleetGrid'
import { HostDrawer } from './components/HostDrawer'
import { RightRail } from './components/RightRail'
import { SwitchDrawer } from './components/SwitchDrawer'
import { TopBar } from './components/TopBar'
import { Topology } from './components/Topology'
import { useStore } from './store'

const VIEWS = ['fleet', 'topology'] as const
type ViewName = (typeof VIEWS)[number]

/** The view named by the URL hash (`#topology`), so a link can open it directly. */
function viewFromHash(hash: string): ViewName {
  return VIEWS.find((v) => `#${v}` === hash) ?? 'fleet'
}

export default function App() {
  const [view, setView] = useState<ViewName>(() => viewFromHash(location.hash))
  const [host, setHost] = useState<string | null>(null)
  const [sw, setSwitch] = useState<string | null>(null)
  const hostExists = useStore((s) => host !== null && host in s.view.hosts)
  const switchExists = useStore((s) => sw !== null && sw in s.view.switches)
  // A removed host or switch (or a preset reset) must not stay selected.
  useEffect(() => {
    if (host !== null && !hostExists) setHost(null)
  }, [host, hostExists])
  useEffect(() => {
    if (sw !== null && !switchExists) setSwitch(null)
  }, [sw, switchExists])
  useEffect(() => {
    const onHash = () => setView(viewFromHash(location.hash))
    window.addEventListener('hashchange', onHash)
    return () => window.removeEventListener('hashchange', onHash)
  }, [])

  // One drawer at a time.
  const selectHost = (id: string) => {
    setHost(id)
    setSwitch(null)
  }
  const selectSwitch = (id: string) => {
    setSwitch(id)
    setHost(null)
  }

  return (
    <div className="app">
      <TopBar />
      <div className="workspace">
        <div className="center">
          <nav className="view-switch" aria-label="view">
            {VIEWS.map((v) => (
              <button key={v} className={v === view ? 'active' : ''} onClick={() => (location.hash = v)}>
                {v === 'fleet' ? 'Fleet' : 'Topology'}
              </button>
            ))}
          </nav>
          <div className="grid-wrap">
            <main className="grid-area">
              {view === 'fleet' ? (
                <FleetGrid selected={host} onSelect={selectHost} />
              ) : (
                <Topology selectedSwitch={sw} selectedHost={host} onSelectSwitch={selectSwitch} onSelectHost={selectHost} />
              )}
            </main>
            {host && <HostDrawer key={host} id={host} onClose={() => setHost(null)} />}
            {sw && <SwitchDrawer key={sw} id={sw} onClose={() => setSwitch(null)} onSelectHost={selectHost} />}
          </div>
          <FaultPanel selected={host} />
        </div>
        <RightRail onSelect={selectHost} />
      </div>
    </div>
  )
}
