import { useState } from 'react'
import { ALL_ROLES } from '../eligibility'
import { BLANK_APP, hostsWithRoles, newAppProblems, newAppYaml, type NewApp } from '../newapp'
import { useStore } from '../store'

/**
 * A typical homelab app from an image, as a form. It only writes the YAML:
 * `onWrite` hands it to the Apply dialog, to preview and apply like any other.
 */
export function NewAppForm({ namespace, namespaces, onWrite }: { namespace: string; namespaces: string[]; onWrite(yaml: string, namespace: string): void }) {
  const [app, setApp] = useState<NewApp>({ ...BLANK_APP, namespace })
  const fleet = useStore((s) => s.view.hosts)
  const fits = hostsWithRoles(fleet, app.roles)
  const set = (patch: Partial<NewApp>) => setApp((a) => ({ ...a, ...patch }))
  const problems = newAppProblems(app)
  const ns = app.namespace || 'default'
  // Namespaces are known by their pods; `default` always exists. Applying an existing one changes nothing.
  const isNew = ns !== 'default' && !namespaces.includes(ns)
  const field = (key: keyof NewApp, label: string, placeholder = '', size = 24) => (
    <input value={app[key]} onChange={(e) => set({ [key]: e.target.value })} placeholder={placeholder} size={size} aria-label={label} />
  )

  return (
    <div>
      <div className="form-grid wide">
        <label>Name</label>
        <span>{field('name', 'name', 'hello')}</span>
        <label>Namespace</label>
        <span>
          <input value={app.namespace} onChange={(e) => set({ namespace: e.target.value.trim() })} placeholder="default" list="apply-namespaces" size={24} aria-label="namespace" />
          {isNew && <span className="muted"> no pods there yet: the YAML creates it</span>}
        </span>
        <label>Image</label>
        <span>{field('image', 'image', 'nginx:1.29', 40)}</span>
        <label>Replicas</label>
        <span>{field('replicas', 'replicas', '1', 4)}</span>
        <label>Port</label>
        <span>
          {field('port', 'port', 'none', 6)} <span className="muted">the container serves on; gets a Service of the same name</span>
        </span>
        <label>Web path</label>
        <span>
          {field('ingressPath', 'web path', 'none', 12)} <span className="muted">served at http://&lt;VIP&gt;&lt;path&gt; through K3s's Traefik; the app sees the path as it is</span>
        </span>
        <label>Volume</label>
        <span>
          {field('volumeGi', 'volume GiB', 'none', 4)} GiB at {field('mountPath', 'mounted at', '/data', 16)}{' '}
          <span className="muted">a Longhorn volume (3 replicas) per pod: a StatefulSet, on hosts on the storage network</span>
        </span>
        <label>Run on</label>
        <span className="role-picker">
          {ALL_ROLES.map((r) => (
            <label key={r} className="inline">
              <input
                type="checkbox"
                checked={app.roles.includes(r)}
                onChange={(e) => set({ roles: ALL_ROLES.filter((x) => (x === r ? e.target.checked : app.roles.includes(x))) })}
                aria-label={`run on ${r} hosts`}
              />
              {r}
            </label>
          ))}
          <span className={app.roles.length > 0 && fits.length === 0 ? 'warn' : 'muted'}>
            {app.roles.length === 0 ? 'any host' : fits.length === 0 ? 'no host has all of these: the pods would wait' : `hosts with all of these: ${fits.join(', ')}`}
          </span>
        </span>
      </div>
      <p className="muted">Pods move off a host that stops answering after 20 s.</p>
      {problems.length > 0 && app.name + app.image !== '' && (
        <ul className="fixes warn">
          {problems.map((p) => (
            <li key={p}>{p}</li>
          ))}
        </ul>
      )}
      <div className="actions">
        <div className="spacer" />
        <button className="active" disabled={problems.length > 0} onClick={() => onWrite(newAppYaml(app, isNew), ns)}>
          Write YAML
        </button>
      </div>
    </div>
  )
}
