import type { KubePod } from '../gen/KubePod'
import { canShell } from '../kube'
import { dock, logsSession, terminalSession } from '../dock'
import { Icon } from './Icons'

/** A pod's Logs and Shell, opening in the dock. */
export function PodButtons({ pod }: { pod: KubePod }) {
  return (
    <span className="pod-buttons" onClick={(e) => e.stopPropagation()}>
      <button className="small" onClick={() => dock.open(logsSession(pod))} title={`Follow ${pod.name}'s log in the dock`} aria-label={`logs of ${pod.name}`}>
        <Icon name="logs" size={14} /> <span className="btn-label">Logs</span>
      </button>
      {canShell(pod) && (
        <button className="small" onClick={() => dock.open(terminalSession(pod))} title={`A shell in ${pod.name}, in the dock`} aria-label={`shell in ${pod.name}`}>
          <Icon name="terminal" size={14} /> <span className="btn-label">Shell</span>
        </button>
      )}
    </span>
  )
}
