import { useState } from 'react'
import { labConsole, labJournal } from '../api'
import { JOURNAL_UNITS, labTerminalUrl } from '../debug'
import type { HostObj } from '../gen/HostObj'
import { useStore } from '../store'
import { Terminal } from './Terminal'
import { useAction } from './useAction'

type Shown = { title: string; text: string } | null

/**
 * Debugging the machine itself, in the lab: what it printed on its serial
 * console while booting, its journal for the units homelabd runs, and an
 * SSH login. All three ride the lab (the console file, the technician's
 * SSH); a real host has no such channel in homelabd yet, so without the
 * lab this says to SSH in by hand.
 */
export function LabDebug({ host }: { host: HostObj }) {
  const inLab = useStore((s) => s.truth.vms.some((v) => v.id === host.id))
  const [unit, setUnit] = useState<string>(JOURNAL_UNITS[0])
  const [shown, setShown] = useState<Shown>(null)
  const [terminal, setTerminal] = useState(false)
  const { error, busy, run } = useAction()

  if (!inLab) {
    return (
      <div className="section">
        <h4>Debug</h4>
        <p className="muted">
          Console, journal and terminal here ride the VM lab; this host is not one of its VMs. On a real machine, SSH in:
          <code> journalctl -u homelabd</code> and <code>journalctl -u k3s</code> say why it is stuck.
        </p>
      </div>
    )
  }

  const show = (title: string, get: () => Promise<string>) =>
    run(async () => setShown({ title, text: await get() }))

  return (
    <div className="section">
      <h4>Debug (lab VM)</h4>
      <div className="actions">
        <button disabled={busy} onClick={() => show('Serial console', () => labConsole(host.id))}>
          Console log
        </button>
        <label className="inline">
          Journal{' '}
          <select value={unit} onChange={(e) => setUnit(e.target.value)} aria-label="journal unit">
            {JOURNAL_UNITS.map((u) => (
              <option key={u}>{u}</option>
            ))}
          </select>
        </label>
        <button disabled={busy} onClick={() => show(`journalctl -u ${unit}`, () => labJournal(host.id, unit, 200))}>
          Read journal
        </button>
        <button disabled={terminal} onClick={() => setTerminal(true)}>
          Terminal (SSH)
        </button>
      </div>
      {error && <div className="bad">{error}</div>}
      {shown && (
        <>
          <div className="muted">{shown.title}</div>
          <pre className="log-view" aria-label={shown.title}>
            {shown.text || '(nothing yet)'}
          </pre>
        </>
      )}
      {terminal && <Terminal url={labTerminalUrl(host.id)} title={`ssh debian@${host.id}`} onClose={() => setTerminal(false)} />}
    </div>
  )
}
