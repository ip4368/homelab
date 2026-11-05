import type { Endpoint } from '../gen/Endpoint'
import type { Health } from '../gen/Health'
import { HEALTH_LABELS, healthTone, usage, usageTone } from '../kube'
import { formatBps } from '../metrics'

/** Host names as links that open the host drawer. */
export function Hosts({ hosts, onSelectHost }: { hosts: string[]; onSelectHost(id: string): void }) {
  if (hosts.length === 0) return <span className="muted">none</span>
  return (
    <>
      {hosts.map((h, n) => (
        <span key={h} className="host-link">
          <button className="link" onClick={() => onSelectHost(h)}>
            {h}
          </button>
          {n < hosts.length - 1 && ','}
        </span>
      ))}
    </>
  )
}

/** `● degraded · CrashLoopBackOff`, the reason cut short unless `full`. */
export function HealthBadge({ health, full }: { health: Health; full?: boolean }) {
  const reason = health.reason && !full && health.reason.length > 48 ? `${health.reason.slice(0, 47)}…` : health.reason
  return (
    <span title={health.reason ?? undefined}>
      <span className={healthTone(health.state)}>● {HEALTH_LABELS[health.state]}</span>
      {reason && <span className="muted"> · {reason}</span>}
    </span>
  )
}

/** Usage, against its limit when there is one, coloured as it nears it. */
export function Usage({ used, limit, format }: { used: number | null; limit: number | null; format(n: number | null): string }) {
  return <span className={usageTone(used, limit)}>{usage(used, limit, format)}</span>
}

/** An endpoint: a link when it is likely a web page, greyed when only reachable inside the cluster. */
export function EndpointLink({ e }: { e: Endpoint }) {
  if (e.link)
    return (
      <a href={e.link} target="_blank" rel="noreferrer" title={e.via}>
        {e.address}
      </a>
    )
  return (
    <span className={e.kind === 'Cluster' ? 'muted' : undefined} title={e.kind === 'Cluster' ? `${e.via}: only from inside the cluster` : e.via}>
      {e.address}
    </span>
  )
}

/** Receive and send rates on two lines, so a narrow column holds them; a dash before there are any. */
export function NetRate({ rx, tx }: { rx: number | null; tx: number | null }) {
  if (rx === null && tx === null) return <span>–</span>
  return (
    <span className="net-rate">
      <span>↓ {formatBps(rx)}</span> <span>↑ {formatBps(tx)}</span>
    </span>
  )
}
