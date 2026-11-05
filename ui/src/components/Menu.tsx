import { useEffect, useRef, useState } from 'react'
import { Icon } from './Icons'

export interface MenuItem {
  label: string
  onClick(): void
  disabled?: boolean
  title?: string
  danger?: boolean
}

/** A "⋯" button opening a short list of actions, for rows that would otherwise wear a row of buttons. */
export function Menu({ label, items }: { label: string; items: (MenuItem | false | null | undefined)[] }) {
  const [at, setAt] = useState<{ top: number; right: number } | null>(null)
  const button = useRef<HTMLButtonElement>(null)
  const list = useRef<HTMLDivElement>(null)
  const shown = items.filter((i): i is MenuItem => !!i)

  useEffect(() => {
    if (!at) return
    const close = (e: Event) => {
      if (e instanceof KeyboardEvent ? e.key === 'Escape' : !list.current?.contains(e.target as Node) && !button.current?.contains(e.target as Node)) setAt(null)
    }
    const onScroll = () => setAt(null)
    document.addEventListener('pointerdown', close)
    document.addEventListener('keydown', close)
    window.addEventListener('resize', onScroll)
    document.addEventListener('scroll', onScroll, true)
    list.current?.querySelector<HTMLButtonElement>('button:not(:disabled)')?.focus()
    return () => {
      document.removeEventListener('pointerdown', close)
      document.removeEventListener('keydown', close)
      window.removeEventListener('resize', onScroll)
      document.removeEventListener('scroll', onScroll, true)
    }
  }, [at])

  if (shown.length === 0) return null
  const open = () => {
    const r = button.current!.getBoundingClientRect()
    setAt(at ? null : { top: r.bottom + 4, right: window.innerWidth - r.right })
  }
  return (
    <>
      <button
        ref={button}
        className="icon-button"
        aria-label={label}
        aria-haspopup="menu"
        aria-expanded={at !== null}
        title={label}
        onClick={(e) => {
          e.stopPropagation()
          open()
        }}
      >
        <Icon name="more" />
      </button>
      {at && (
        <div
          ref={list}
          className="menu"
          role="menu"
          style={{ top: Math.min(at.top, window.innerHeight - 40 * shown.length - 12), right: at.right }}
          onClick={(e) => e.stopPropagation()}
        >
          {shown.map((i) => (
            <button
              key={i.label}
              role="menuitem"
              className={i.danger ? 'danger' : undefined}
              disabled={i.disabled}
              title={i.title}
              onClick={() => {
                setAt(null)
                i.onClick()
              }}
            >
              {i.label}
            </button>
          ))}
        </div>
      )}
    </>
  )
}
