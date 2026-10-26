import { useState } from 'react'
import { op } from '../api'
import type { BondMode } from '../gen/BondMode'
import type { HostNetworkConfig } from '../gen/HostNetworkConfig'
import type { HostObj } from '../gen/HostObj'
import {
  addAttachment,
  addBond,
  addSingle,
  configDiff,
  describeIssue,
  formProblems,
  linkPorts,
  moveAttachment,
  portOwners,
  removeAttachment,
  removeLink,
  setIp,
  setMtu,
  setTagged,
} from '../netedit'
import { useStore } from '../store'
import { useAction } from './useAction'

const EMPTY: HostNetworkConfig = { links: [], attachments: [] }

interface FormProps {
  cfg: HostNetworkConfig
  onChange(cfg: HostNetworkConfig): void
  /** The NICs the config can use. */
  ports: string[]
  /** Hosts have addresses; a template does not. */
  showIp: boolean
}

/** The form for a network config: ports, then the links on them, then the networks on the links. */
export function ConfigForm({ cfg, onChange, ports, showIp }: FormProps) {
  const networks = useStore((s) => s.view.networks)
  const owners = portOwners(cfg)
  const [bondName, setBondName] = useState('bond0')
  const [bondPorts, setBondPorts] = useState<string[]>([])
  const [bondMode, setBondMode] = useState<BondMode>('Lacp')
  const [newNet, setNewNet] = useState('')
  const [newLink, setNewLink] = useState('')

  const singles = cfg.links.flatMap((l) => ('Single' in l.kind ? [l.kind.Single] : []))
  const unattached = Object.keys(networks)
    .filter((n) => !cfg.attachments.some((a) => a.network === n))
    .sort()
  const networkToAdd = newNet || unattached[0] || ''
  const linkToAdd = cfg.links.some((l) => l.name === newLink) ? newLink : (cfg.links[0]?.name ?? '')
  const bondable = [...ports.filter((p) => !owners.has(p)), ...singles]
  const problems = formProblems(cfg)

  const toggleBondPort = (p: string, on: boolean) => setBondPorts(on ? [...bondPorts, p] : bondPorts.filter((x) => x !== p))
  const makeBond = () => {
    onChange(addBond(cfg, bondName.trim(), bondPorts, bondMode))
    setBondPorts([])
  }

  return (
    <div className="config-form">
      <h5>Ports</h5>
      <ul className="plain">
        {ports.map((p) => (
          <li key={p}>
            <code>{p}</code>{' '}
            {owners.has(p) ? (
              <span className="muted">on link {owners.get(p)}</span>
            ) : (
              <button onClick={() => onChange(addSingle(cfg, p))}>Add link</button>
            )}
          </li>
        ))}
        {ports.length === 0 && <li className="muted">No ports known for this hardware.</li>}
      </ul>
      {bondable.length >= 2 && (
        <div className="bond-form">
          <span className="muted">Bond:</span>
          {bondable.map((p) => (
            <label key={p} className="inline">
              <input type="checkbox" checked={bondPorts.includes(p)} onChange={(e) => toggleBondPort(p, e.target.checked)} /> {p}
            </label>
          ))}
          <input value={bondName} onChange={(e) => setBondName(e.target.value)} size={7} aria-label="bond name" />
          <select value={bondMode} onChange={(e) => setBondMode(e.target.value as BondMode)} aria-label="bond mode">
            <option>Lacp</option>
            <option>ActiveBackup</option>
          </select>
          <button disabled={bondPorts.length < 2 || bondName.trim() === '' || cfg.links.some((l) => l.name === bondName.trim())} onClick={makeBond}>
            Create bond
          </button>
        </div>
      )}

      <h5>Links</h5>
      <table className="ports">
        <tbody>
          {cfg.links.map((l) => (
            <tr key={l.name}>
              <td>
                <strong>{l.name}</strong>
              </td>
              <td>{'Single' in l.kind ? 'port ' + l.kind.Single : `${l.kind.Bond.mode} bond of ${linkPorts(l.kind).join(' + ')}`}</td>
              <td>
                MTU{' '}
                <input
                  type="number"
                  value={l.mtu}
                  onChange={(e) => onChange(setMtu(cfg, l.name, Number(e.target.value)))}
                  style={{ width: 64 }}
                  aria-label={`${l.name} mtu`}
                />
              </td>
              <td>
                <button onClick={() => onChange(removeLink(cfg, l.name))} aria-label={`remove link ${l.name}`}>
                  Remove
                </button>
              </td>
            </tr>
          ))}
          {cfg.links.length === 0 && (
            <tr>
              <td className="muted">No links.</td>
            </tr>
          )}
        </tbody>
      </table>

      <h5>Networks</h5>
      <table className="ports">
        <tbody>
          {cfg.attachments.map((a) => (
            <tr key={a.network}>
              <td>
                <strong>{a.network}</strong>
                {!(a.network in networks) && <span className="bad"> unknown</span>}
              </td>
              <td>
                <select value={a.link} onChange={(e) => onChange(moveAttachment(cfg, a.network, e.target.value))} aria-label={`${a.network} link`}>
                  {!cfg.links.some((l) => l.name === a.link) && <option value={a.link}>{a.link} (missing)</option>}
                  {cfg.links.map((l) => (
                    <option key={l.name}>{l.name}</option>
                  ))}
                </select>
              </td>
              <td>
                <label className="inline">
                  <input type="checkbox" checked={a.tagged} onChange={(e) => onChange(setTagged(cfg, a.network, e.target.checked))} /> tagged
                </label>
              </td>
              {showIp && (
                <td>
                  <input
                    value={a.ip ?? ''}
                    placeholder="no address"
                    size={11}
                    onChange={(e) => onChange(setIp(cfg, a.network, e.target.value))}
                    aria-label={`${a.network} address`}
                  />
                </td>
              )}
              <td>
                <button onClick={() => onChange(removeAttachment(cfg, a.network))} aria-label={`detach ${a.network}`}>
                  Detach
                </button>
              </td>
            </tr>
          ))}
          {cfg.attachments.length === 0 && (
            <tr>
              <td className="muted">No networks attached.</td>
            </tr>
          )}
        </tbody>
      </table>
      {unattached.length > 0 && cfg.links.length > 0 && (
        <div className="actions">
          <select value={networkToAdd} onChange={(e) => setNewNet(e.target.value)} aria-label="network to attach">
            {unattached.map((n) => (
              <option key={n}>{n}</option>
            ))}
          </select>
          on
          <select value={linkToAdd} onChange={(e) => setNewLink(e.target.value)} aria-label="link to attach to">
            {cfg.links.map((l) => (
              <option key={l.name}>{l.name}</option>
            ))}
          </select>
          <button onClick={() => onChange(addAttachment(cfg, networkToAdd, linkToAdd, networks[networkToAdd]?.vlan !== 1))}>Attach</button>
        </div>
      )}
      {problems.length > 0 && (
        <ul className="problems">
          {problems.map((p) => (
            <li key={p} className="bad">
              {p}
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}

const same = (a: HostNetworkConfig, b: HostNetworkConfig) => JSON.stringify(a) === JSON.stringify(b)

/** A host's network: status, drift from its template, validation issues, and the override form. */
export function NetworkEditor({ host }: { host: HostObj }) {
  const template = useStore((s) => (host.template ? s.view.templates[host.template] : undefined))
  const { net } = host
  const start = host.override_ ?? net.desired ?? EMPTY
  const [draft, setDraft] = useState<HostNetworkConfig>(start)
  const { error, busy, run } = useAction()
  const problems = formProblems(draft)
  const heldRoles = Object.entries(net.held_roles)
  const drift = host.override_ && template ? configDiff(template.config, host.override_) : []

  return (
    <div className="section network-editor">
      <h4>Network</h4>
      <p className="muted">
        Template: {host.template ?? 'none (bootstrap)'}. State <strong className={net.state === 'Applied' ? 'ok' : net.state === 'Applying' || net.state === 'Pending' ? 'warn' : 'bad'}>{net.state}</strong>, generation{' '}
        {net.applied_generation} of {net.desired_generation}.{host.override_ ? ' Has an override.' : ''}
      </p>
      {net.error && <div className="bad">{net.error}</div>}
      {host.override_ && (
        <div>
          <strong className={net.drift ? 'warn' : ''}>{net.drift ? 'Drift from template' : 'Override matches template'}</strong>
          {drift.length > 0 && (
            <ul className="fixes">
              {drift.map((d) => (
                <li key={d}>{d}</li>
              ))}
            </ul>
          )}
        </div>
      )}
      {net.issues.length > 0 && (
        <div>
          <strong className="bad">Issues</strong>
          <ul className="fixes">
            {net.issues.map((i, n) => (
              <li key={n}>{describeIssue(i)}</li>
            ))}
          </ul>
        </div>
      )}
      {net.fixes.length > 0 && (
        <div>
          <strong>Suggested fixes</strong>
          <ul className="fixes">
            {net.fixes.map((f) => (
              <li key={f}>{f}</li>
            ))}
          </ul>
        </div>
      )}
      {heldRoles.length > 0 && (
        <p className="warn">Roles held inactive: {heldRoles.map(([role, network]) => `${role} (needs ${network})`).join(', ')}</p>
      )}
      <ConfigForm cfg={draft} onChange={setDraft} ports={host.inventory.nics.map((n) => n.name)} showIp />
      <div className="actions">
        <button disabled={busy || problems.length > 0 || same(draft, start)} onClick={() => run(() => op({ SetHostOverride: { id: host.id, config: draft } }))}>
          Save override
        </button>
        <button disabled={busy || same(draft, start)} onClick={() => setDraft(start)}>
          Reset
        </button>
        <button disabled={busy || host.override_ === null} onClick={() => run(() => op({ SetHostOverride: { id: host.id, config: null } }))}>
          Clear override
        </button>
        {error && <span className="bad">{error}</span>}
      </div>
    </div>
  )
}
