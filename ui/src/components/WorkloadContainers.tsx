import type { ContainerSpec } from '../gen/ContainerSpec'
import type { KubePod } from '../gen/KubePod'
import type { LonghornVolume } from '../gen/LonghornVolume'
import type { Revision } from '../gen/Revision'
import { age, claimVolume, cpu, gib, mem, mountSource, requestLimit, shortDigest } from '../kube'

const ROLE_LABELS: Record<ContainerSpec['role'], string | null> = { App: null, Init: 'init', Sidecar: 'sidecar' }

/**
 * A workload's containers as its pod template asks for them: image (and the
 * digests its pods run), resources, ports, probes, environment, mounts and devices.
 */
export function Containers({
  namespace,
  containers,
  digests,
  volumes,
}: {
  namespace: string
  containers: ContainerSpec[]
  digests: Map<string, string[]>
  volumes: LonghornVolume[]
}) {
  return (
    <div className="containers">
      {containers.map((c) => {
        const ran = digests.get(c.name) ?? []
        const cpuRL = requestLimit(c.cpu_request_milli, c.cpu_limit_milli, cpu)
        const memRL = requestLimit(c.memory_request_bytes, c.memory_limit_bytes, mem)
        return (
          <div key={c.name} className="container-card">
            <div>
              <strong>{c.name}</strong>
              {ROLE_LABELS[c.role] && <span className="muted"> ({ROLE_LABELS[c.role]})</span>} <code>{c.image}</code>
              {ran.length === 1 && <span className="muted" title={ran[0]}> {shortDigest(ran[0])}</span>}
              {ran.length > 1 && (
                <div className="warn" title={ran.join('\n')}>
                  its pods run {ran.length} different images: {ran.map(shortDigest).join(', ')}
                </div>
              )}
            </div>
            <div className="form-grid">
              {(cpuRL || memRL) && (
                <>
                  <label>Request / limit</label>
                  <span>
                    {cpuRL && <span>CPU {cpuRL}</span>}
                    {cpuRL && memRL && ', '}
                    {memRL && <span>memory {memRL}</span>}
                  </span>
                </>
              )}
              {c.ports.length > 0 && (
                <>
                  <label>Ports</label>
                  <span>
                    {c.ports.map((p, n) => (
                      <span key={n}>
                        {n > 0 && ', '}
                        {p.name && `${p.name} `}
                        {p.port}/{p.protocol}
                        {p.host_port !== null && <span className="warn" title="takes this port on its host"> on host :{p.host_port}</span>}
                      </span>
                    ))}
                  </span>
                </>
              )}
              {c.probes.map((p) => (
                <ProbeRow key={p.kind} kind={p.kind} check={p.check} every={p.every_s} failures={p.failures} />
              ))}
              {c.security.length > 0 && (
                <>
                  <label>Security</label>
                  <span className="warn">{c.security.join(', ')}</span>
                </>
              )}
              {c.devices.length > 0 && (
                <>
                  <label>Devices</label>
                  <span>{c.devices.map((d) => d.what).join(', ')}</span>
                </>
              )}
              {c.env.length > 0 && (
                <>
                  <label>Environment</label>
                  <span className="env-list">
                    {c.env.map((e, n) => (
                      <span key={n} title={e.from}>
                        {n > 0 && ', '}
                        <code>{e.name}</code>
                        {e.from !== 'literal' && <span className="muted"> ← {e.from}</span>}
                      </span>
                    ))}
                  </span>
                </>
              )}
            </div>
            {c.mounts.length > 0 && (
              <table className="data mounts">
                <tbody>
                  {c.mounts.map((m) => {
                    const v = m.kind === 'Claim' && m.source ? claimVolume(namespace, m.source, volumes) : undefined
                    return (
                      <tr key={m.path}>
                        <td>
                          <code>{m.path}</code>
                          {m.sub_path && <span className="muted"> ({m.sub_path})</span>}
                        </td>
                        <td className={m.kind === 'HostPath' ? 'warn' : undefined}>
                          {mountSource(m)}
                          {v && <span className="muted"> · Longhorn {gib(v.size_bytes)}</span>}
                        </td>
                        <td className="muted">{m.read_only ? 'read-only' : ''}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            )}
          </div>
        )
      })}
    </div>
  )
}

function ProbeRow({ kind, check, every, failures }: { kind: string; check: string; every: number; failures: number }) {
  return (
    <>
      <label>{kind}</label>
      <span>
        <code>{check}</code>{' '}
        <span className="muted">
          every {every} s, {failures} failure{failures === 1 ? '' : 's'}
        </span>
      </span>
    </>
  )
}

/** A pod in more detail, under its row: each container's state and digest, its IPs, networks and QoS class. */
export function PodDetail({ pod }: { pod: KubePod }) {
  return (
    <div className="pod-detail">
      {pod.container_states.map((s) => (
        <div key={s.name}>
          <strong>{s.name}</strong>
          {ROLE_LABELS[s.role] && <span className="muted"> ({ROLE_LABELS[s.role]})</span>}{' '}
          <span className={s.state === 'running' ? (s.ready ? 'ok' : 'warn') : s.state === 'Completed' ? 'muted' : 'bad'}>{s.state}</span>
          {s.restarts > 0 && <span className="warn"> {s.restarts} restarts</span>}
          {s.image_id && (
            <span className="muted" title={s.image_id}>
              {' '}
              {shortDigest(s.image_id)}
            </span>
          )}
        </div>
      ))}
      <div className="muted">
        {pod.ips.length > 0 ? `IP ${pod.ips.join(', ')}` : 'no IP yet'}
        {pod.qos && ` · QoS ${pod.qos}`}
      </div>
      {pod.networks
        .filter((n) => !n.default)
        .map((n) => (
          <div key={n.name} className="muted">
            {n.interface ?? '?'} on {n.name}: {n.ips.join(', ') || 'no IP'}
            {n.mac && ` (${n.mac})`}
          </div>
        ))}
    </div>
  )
}

/** A workload's rollout history, newest first. */
export function History({ revisions, now }: { revisions: Revision[]; now: number }) {
  return (
    <table className="data">
      <thead>
        <tr>
          <th>Revision</th>
          <th>Images</th>
          <th>Age</th>
          <th>Pods</th>
        </tr>
      </thead>
      <tbody>
        {revisions.map((r) => (
          <tr key={r.number} className={r.current ? undefined : 'muted'}>
            <td>
              {r.number}
              {r.current && <span className="ok"> current</span>}
            </td>
            <td>
              {r.images.map((i) => (
                <div key={i}>
                  <code>{i}</code>
                </div>
              ))}
            </td>
            <td>{age(r.created, now)}</td>
            <td>{r.pods ?? ''}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}
