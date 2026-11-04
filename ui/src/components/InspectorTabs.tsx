interface Props<T extends string> {
  tabs: readonly T[]
  value: T
  onChange(tab: T): void
  label: string
}

/** Compact tabs that keep a docked inspector task-sized instead of one long scroll. */
export function InspectorTabs<T extends string>({ tabs, value, onChange, label }: Props<T>) {
  return (
    <div className="inspector-tabs" role="tablist" aria-label={label}>
      {tabs.map((tab) => (
        <button key={tab} role="tab" aria-selected={tab === value} className={tab === value ? 'active' : ''} onClick={() => onChange(tab)}>
          {tab}
        </button>
      ))}
    </div>
  )
}
