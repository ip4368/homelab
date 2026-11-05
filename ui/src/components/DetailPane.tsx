import { useRef, type ReactNode } from 'react'
import { usePref } from '../prefs'
import { Icon } from './Icons'

export interface PaneTab {
  id: string
  label: string
  /** A count after the label, e.g. pods; omitted when undefined. */
  count?: number
  /** Colours the count: warnings on a workload, say. */
  tone?: 'warn' | 'bad'
}

const MIN_WIDTH = 380
/** The list beside the pane keeps at least this much. */
const LIST_MIN = 420

/**
 * The detail pane beside a page's list: a header saying what is open and how
 * it is, its actions, and tabs, so one thing's details take a screen's width
 * in sections rather than one long column. Drag its left edge to resize it;
 * on a narrow screen it covers the list.
 */
export function DetailPane({
  label,
  title,
  status,
  subtitle,
  actions,
  tabs,
  tab,
  onTab,
  onClose,
  children,
}: {
  label: string
  title: ReactNode
  status?: ReactNode
  subtitle?: ReactNode
  actions?: ReactNode
  tabs: PaneTab[]
  tab: string | null
  onTab(id: string): void
  onClose(): void
  children(tab: string): ReactNode
}) {
  const [width, setWidth] = usePref('paneWidth', 680)
  const drag = useRef<{ x: number; w: number } | null>(null)
  const shown = tabs.some((t) => t.id === tab) ? tab! : tabs[0].id

  const onDown = (e: React.PointerEvent) => {
    drag.current = { x: e.clientX, w: width }
    ;(e.target as HTMLElement).setPointerCapture(e.pointerId)
  }
  const onMove = (e: React.PointerEvent) => {
    if (!drag.current) return
    const max = Math.max(MIN_WIDTH, (e.currentTarget.parentElement?.parentElement?.clientWidth ?? window.innerWidth) - LIST_MIN)
    setWidth(Math.round(Math.max(MIN_WIDTH, Math.min(max, drag.current.w + drag.current.x - e.clientX))))
  }

  return (
    <aside className="pane" aria-label={label} style={{ '--pane-w': `${width}px` } as React.CSSProperties}>
      <div className="pane-resize" onPointerDown={onDown} onPointerMove={onMove} onPointerUp={() => (drag.current = null)} title="drag to resize" />
      <header className="pane-head">
        <div className="pane-title">
          <h2>{title}</h2>
          {status}
          <div className="spacer" />
          <button className="icon-button" onClick={onClose} aria-label="close" title="Close (Esc)">
            <Icon name="close" />
          </button>
        </div>
        {subtitle && <div className="pane-sub">{subtitle}</div>}
        {actions && <div className="pane-actions">{actions}</div>}
        {tabs.length > 1 && (
          <nav className="pane-tabs" role="tablist" aria-label={`${label} sections`}>
            {tabs.map((t) => (
              <button key={t.id} role="tab" aria-selected={t.id === shown} className={t.id === shown ? 'active' : ''} onClick={() => onTab(t.id)}>
                {t.label}
                {t.count !== undefined && <span className={`count${t.tone ? ` ${t.tone}` : ''}`}>{t.count}</span>}
              </button>
            ))}
          </nav>
        )}
      </header>
      <div className="pane-body" key={shown}>
        {children(shown)}
      </div>
    </aside>
  )
}

/** A titled block inside a pane or page. */
export function Card({ title, extra, children, className }: { title?: ReactNode; extra?: ReactNode; children: ReactNode; className?: string }) {
  return (
    <section className={`card${className ? ` ${className}` : ''}`}>
      {(title || extra) && (
        <header className="card-head">
          {title && <h3>{title}</h3>}
          <div className="spacer" />
          {extra}
        </header>
      )}
      {children}
    </section>
  )
}
