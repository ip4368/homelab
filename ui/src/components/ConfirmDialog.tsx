import type { ReactNode } from 'react'

interface Props {
  /** The question, e.g. "Remove ms01-04 from the control plane?" */
  title: string
  /** What the button that goes ahead says, e.g. "Remove". */
  confirmLabel: string
  /** Why it matters, shown above the buttons. */
  children?: ReactNode
  onConfirm(): void
  onCancel(): void
}

/** In-app confirmation dialog, on the shared modal styles; used instead of window.confirm. */
export function ConfirmDialog({ title, confirmLabel, children, onConfirm, onCancel }: Props) {
  return (
    <div className="modal-backdrop" onClick={onCancel}>
      <div className="modal narrow" role="dialog" aria-label={title} onClick={(e) => e.stopPropagation()}>
        <div className="drawer-head">
          <h3>{title}</h3>
          <div className="spacer" />
          <button onClick={onCancel} aria-label="close">
            ×
          </button>
        </div>
        {children && <div>{children}</div>}
        <div className="actions">
          <button className="active" onClick={onConfirm}>
            {confirmLabel}
          </button>
          <button onClick={onCancel}>Cancel</button>
        </div>
      </div>
    </div>
  )
}
