import { useState } from 'react'
import { FaultPanel } from './components/FaultPanel'
import { FleetGrid } from './components/FleetGrid'
import { HostDrawer } from './components/HostDrawer'
import { RightRail } from './components/RightRail'
import { TopBar } from './components/TopBar'

export default function App() {
  const [selected, setSelected] = useState<string | null>(null)
  return (
    <div className="app">
      <TopBar />
      <div className="workspace">
        <div className="center">
          <div className="grid-wrap">
            <main className="grid-area">
              <FleetGrid selected={selected} onSelect={setSelected} />
            </main>
            {selected && <HostDrawer key={selected} id={selected} onClose={() => setSelected(null)} />}
          </div>
          <FaultPanel selected={selected} />
        </div>
        <RightRail onSelect={setSelected} />
      </div>
    </div>
  )
}
