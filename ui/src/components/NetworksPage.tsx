import { useMemo, useState } from 'react'
import { op } from '../api'
import { ALL_ROLES } from '../eligibility'
import type { AutoApproveRule } from '../gen/AutoApproveRule'
import type { Change } from '../gen/Change'
import type { HostNetworkConfig } from '../gen/HostNetworkConfig'
import type { NetworkPurpose } from '../gen/NetworkPurpose'
import type { ProfileId } from '../gen/ProfileId'
import type { Role } from '../gen/Role'
import type { TemplateObj } from '../gen/TemplateObj'
import { formFromNetwork, networkFromForm, parseInUse, formProblems, type NetworkForm } from '../netedit'
import { useStore } from '../store'
import { ConfigForm } from './NetworkEditor'
import { RolloutDialog } from './RolloutDialog'
import { useAction } from './useAction'

const TABS = ['Networks', 'Templates', 'Auto-approve rules'] as const
type Tab = (typeof TABS)[number]
const PROFILES: ProfileId[] = ['Ms01', 'MiniPc', 'Pi5', 'Rk3588']
const PURPOSES: NetworkPurpose[] = ['Mgmt', 'Storage', 'Migration', 'Vm']

/** A refused delete: the users named by the server, or its message as it came. */
function Refusal({ what, error }: { what: string; error: string }) {
  const users = parseInUse(error)
  return (
    <div className="bad refusal" role="alert">
      {users ? (
        <>
          Cannot delete {what}: it is in use by
          <ul className="fixes">
            {users.map((u) => (
              <li key={u}>{u}</li>
            ))}
          </ul>
        </>
      ) : (
        error
      )}
    </div>
  )
}

/** Delete button that keeps the server's refusal next to it. */
function useDelete(what: string, send: () => Promise<unknown>) {
  const { error, busy, run } = useAction()
  const button = (
    <button
      disabled={busy}
      onClick={() => {
        if (window.confirm(`Delete ${what}?`)) void run(send)
      }}
    >
      Delete
    </button>
  )
  return { button, refusal: error ? <Refusal what={what} error={error} /> : null }
}

function NetworkRow({ id, onEdit }: { id: string; onEdit(): void }) {
  const n = useStore((s) => s.view.networks[id])
  const del = useDelete(`network ${id}`, () => op({ DeleteNetwork: { id } }))
  if (!n) return null
  return (
    <>
      <tr>
        <td>
          <strong>{n.id}</strong>
        </td>
        <td>{n.purpose}</td>
        <td className="num">{n.vlan}</td>
        <td>{n.subnet}</td>
        <td className="num">{n.mtu}</td>
        <td className="row-actions">
          <button onClick={onEdit}>Edit</button> {del.button}
        </td>
      </tr>
      {del.refusal && (
        <tr>
          <td colSpan={6}>{del.refusal}</td>
        </tr>
      )}
    </>
  )
}

const BLANK_NETWORK: NetworkForm = { id: '', purpose: 'Vm', vlan: '', subnet: '', mtu: '1500' }

function NetworkEditorForm({ initial, existing, onRollout, onCancel }: { initial: NetworkForm; existing: boolean; onRollout(c: Change): void; onCancel(): void }) {
  const [form, setForm] = useState(initial)
  const [invalid, setInvalid] = useState<string | null>(null)
  const set = (patch: Partial<NetworkForm>) => setForm({ ...form, ...patch })
  const submit = () => {
    const parsed = networkFromForm(form)
    setInvalid('error' in parsed ? parsed.error : null)
    if ('network' in parsed) onRollout({ Network: parsed.network })
  }
  return (
    <div className="panel">
      <h4>{existing ? `Edit network ${initial.id}` : 'New network'}</h4>
      <div className="form-grid wide">
        <label>Name</label>
        <input value={form.id} disabled={existing} onChange={(e) => set({ id: e.target.value })} size={14} />
        <label>Purpose</label>
        <select value={form.purpose} onChange={(e) => set({ purpose: e.target.value as NetworkPurpose })}>
          {PURPOSES.map((p) => (
            <option key={p}>{p}</option>
          ))}
        </select>
        <label>VLAN</label>
        <input value={form.vlan} onChange={(e) => set({ vlan: e.target.value })} size={6} />
        <label>Subnet</label>
        <input value={form.subnet} onChange={(e) => set({ subnet: e.target.value })} placeholder="10.40.0.0/24" size={16} />
        <label>MTU</label>
        <input value={form.mtu} onChange={(e) => set({ mtu: e.target.value })} size={6} />
      </div>
      <div className="actions">
        <button className="active" onClick={submit}>
          Review rollout...
        </button>
        <button onClick={onCancel}>Cancel</button>
        {invalid && <span className="bad">{invalid}</span>}
      </div>
    </div>
  )
}

function NetworksTab({ onRollout }: { onRollout(c: Change): void }) {
  const networks = useStore((s) => s.view.networks)
  const [editing, setEditing] = useState<NetworkForm | 'new' | null>(null)
  const ids = Object.keys(networks).sort()
  return (
    <div>
      <table className="ports data">
        <thead>
          <tr>
            <th>Network</th>
            <th>Purpose</th>
            <th>VLAN</th>
            <th>Subnet</th>
            <th>MTU</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {ids.map((id) => (
            <NetworkRow key={id} id={id} onEdit={() => setEditing(formFromNetwork(networks[id]))} />
          ))}
          {ids.length === 0 && (
            <tr>
              <td colSpan={6} className="muted">
                No networks.
              </td>
            </tr>
          )}
        </tbody>
      </table>
      <div className="actions">
        <button onClick={() => setEditing('new')}>New network</button>
      </div>
      {editing && (
        <NetworkEditorForm
          key={editing === 'new' ? 'new' : editing.id}
          initial={editing === 'new' ? BLANK_NETWORK : editing}
          existing={editing !== 'new'}
          onRollout={onRollout}
          onCancel={() => setEditing(null)}
        />
      )}
    </div>
  )
}

function TemplateRow({ t, onEdit }: { t: TemplateObj; onEdit(): void }) {
  const hosts = useStore((s) => s.view.hosts)
  const rules = useStore((s) => s.view.rules)
  const users = useMemo(
    () => ({
      hosts: Object.values(hosts).filter((h) => h.template === t.id).length,
      rules: Object.values(rules).filter((r) => r.template === t.id).length,
    }),
    [hosts, rules, t.id],
  )
  const del = useDelete(`template ${t.id}`, () => op({ DeleteTemplate: { id: t.id } }))
  return (
    <>
      <tr>
        <td>
          <strong>{t.id}</strong>
        </td>
        <td>{t.profile}</td>
        <td>{t.role ?? '-'}</td>
        <td>
          {t.config.links.length} links; {t.config.attachments.map((a) => `${a.network}${a.tagged ? '' : ' (untagged)'}`).join(', ')}
        </td>
        <td className="num">
          {users.hosts} hosts, {users.rules} rules
        </td>
        <td className="row-actions">
          <button onClick={onEdit}>Edit</button> {del.button}
        </td>
      </tr>
      {del.refusal && (
        <tr>
          <td colSpan={6}>{del.refusal}</td>
        </tr>
      )}
    </>
  )
}

interface TemplateDraft {
  id: string
  profile: ProfileId
  role: Role | ''
  config: HostNetworkConfig
}

function TemplateEditorForm({ initial, existing, onRollout, onCancel }: { initial: TemplateDraft; existing: boolean; onRollout(c: Change): void; onCancel(): void }) {
  const hosts = useStore((s) => s.view.hosts)
  const templates = useStore((s) => s.view.templates)
  const [draft, setDraft] = useState(initial)
  const ports = useMemo(() => {
    const names = new Set<string>()
    for (const h of Object.values(hosts)) if (h.profile.id === draft.profile) h.inventory.nics.forEach((n) => names.add(n.name))
    return [...names].sort()
  }, [hosts, draft.profile])
  const problems = formProblems(draft.config)
  const invalid = draft.id.trim() === '' ? 'Name: required' : problems[0]
  const copyFrom = (id: string) => {
    const from = templates[id]
    if (from) setDraft({ ...draft, config: from.config })
  }
  return (
    <div className="panel">
      <h4>{existing ? `Edit template ${initial.id}` : 'New template'}</h4>
      <div className="form-grid wide">
        <label>Name</label>
        <input value={draft.id} disabled={existing} onChange={(e) => setDraft({ ...draft, id: e.target.value })} size={18} />
        <label>Hardware</label>
        <select value={draft.profile} disabled={existing} onChange={(e) => setDraft({ ...draft, profile: e.target.value as ProfileId })}>
          {PROFILES.map((p) => (
            <option key={p}>{p}</option>
          ))}
        </select>
        <label>Role</label>
        <select value={draft.role} onChange={(e) => setDraft({ ...draft, role: e.target.value as Role | '' })}>
          <option value="">any role</option>
          {ALL_ROLES.map((r) => (
            <option key={r}>{r}</option>
          ))}
        </select>
        {!existing && (
          <>
            <label>Copy from</label>
            <select value="" onChange={(e) => copyFrom(e.target.value)}>
              <option value="">-</option>
              {Object.keys(templates)
                .sort()
                .map((id) => (
                  <option key={id}>{id}</option>
                ))}
            </select>
          </>
        )}
      </div>
      <ConfigForm cfg={draft.config} onChange={(config) => setDraft({ ...draft, config })} ports={ports} showIp={false} />
      <div className="actions">
        <button
          className="active"
          disabled={invalid !== undefined}
          onClick={() => onRollout({ Template: { id: draft.id.trim(), profile: draft.profile, role: draft.role === '' ? null : draft.role, config: draft.config } })}
        >
          Review rollout...
        </button>
        <button onClick={onCancel}>Cancel</button>
        {invalid && <span className="muted">{invalid}</span>}
      </div>
    </div>
  )
}

function TemplatesTab({ onRollout }: { onRollout(c: Change): void }) {
  const templates = useStore((s) => s.view.templates)
  const [editing, setEditing] = useState<TemplateDraft | null>(null)
  const [existing, setExisting] = useState(false)
  const rows = Object.values(templates).sort((a, b) => a.id.localeCompare(b.id))
  const edit = (t: TemplateObj) => {
    setExisting(true)
    setEditing({ id: t.id, profile: t.profile, role: t.role ?? '', config: t.config })
  }
  return (
    <div>
      <table className="ports data">
        <thead>
          <tr>
            <th>Template</th>
            <th>Hardware</th>
            <th>Role</th>
            <th>Config</th>
            <th>Used by</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {rows.map((t) => (
            <TemplateRow key={t.id} t={t} onEdit={() => edit(t)} />
          ))}
          {rows.length === 0 && (
            <tr>
              <td colSpan={6} className="muted">
                No templates.
              </td>
            </tr>
          )}
        </tbody>
      </table>
      <div className="actions">
        <button
          onClick={() => {
            setExisting(false)
            setEditing({ id: '', profile: 'Ms01', role: '', config: { links: [], attachments: [] } })
          }}
        >
          New template
        </button>
      </div>
      {editing && <TemplateEditorForm key={`${existing}:${editing.id}`} initial={editing} existing={existing} onRollout={onRollout} onCancel={() => setEditing(null)} />}
    </div>
  )
}

interface RuleDraft {
  id: string
  profile: ProfileId
  roles: Role[]
  template: string
}

function RuleRow({ r, onEdit }: { r: AutoApproveRule; onEdit(): void }) {
  const del = useDelete(`rule ${r.id}`, () => op({ DeleteRule: { id: r.id } }))
  return (
    <>
      <tr>
        <td>
          <strong>{r.id}</strong>
        </td>
        <td>{r.profile}</td>
        <td>{r.roles.join(', ')}</td>
        <td>{r.template ?? <span className="muted">by profile</span>}</td>
        <td className="row-actions">
          <button onClick={onEdit}>Edit</button> {del.button}
        </td>
      </tr>
      {del.refusal && (
        <tr>
          <td colSpan={5}>{del.refusal}</td>
        </tr>
      )}
    </>
  )
}

function RuleEditorForm({ initial, existing, onDone }: { initial: RuleDraft; existing: boolean; onDone(): void }) {
  const templates = useStore((s) => s.view.templates)
  const [draft, setDraft] = useState(initial)
  const { error, busy, run } = useAction()
  const toggle = (role: Role, on: boolean) => setDraft({ ...draft, roles: ALL_ROLES.filter((r) => (r === role ? on : draft.roles.includes(r))) })
  const save = () =>
    run(async () => {
      await op({ UpsertRule: { id: draft.id.trim(), profile: draft.profile, roles: draft.roles, template: draft.template === '' ? null : draft.template } })
      onDone()
    })
  return (
    <div className="panel">
      <h4>{existing ? `Edit rule ${initial.id}` : 'New rule'}</h4>
      <div className="form-grid wide">
        <label>Name</label>
        <input value={draft.id} disabled={existing} onChange={(e) => setDraft({ ...draft, id: e.target.value })} size={14} />
        <label>Hardware</label>
        <select value={draft.profile} onChange={(e) => setDraft({ ...draft, profile: e.target.value as ProfileId })}>
          {PROFILES.map((p) => (
            <option key={p}>{p}</option>
          ))}
        </select>
        <label>Roles</label>
        <span className="role-picker">
          {ALL_ROLES.map((r) => (
            <label key={r} className="inline">
              <input type="checkbox" checked={draft.roles.includes(r)} onChange={(e) => toggle(r, e.target.checked)} /> {r}
            </label>
          ))}
        </span>
        <label>Template</label>
        <select value={draft.template} onChange={(e) => setDraft({ ...draft, template: e.target.value })}>
          <option value="">by profile</option>
          {Object.keys(templates)
            .sort()
            .map((id) => (
              <option key={id}>{id}</option>
            ))}
        </select>
      </div>
      <div className="actions">
        <button className="active" disabled={busy || draft.id.trim() === '' || draft.roles.length === 0} onClick={save}>
          Save rule
        </button>
        <button onClick={onDone}>Cancel</button>
        {error && <span className="bad">{error}</span>}
      </div>
      <p className="muted">Only affects hosts that join afterwards. Roles the hardware cannot take are refused by the server.</p>
    </div>
  )
}

function RulesTab() {
  const rules = useStore((s) => s.view.rules)
  const [editing, setEditing] = useState<{ draft: RuleDraft; existing: boolean } | null>(null)
  const rows = Object.values(rules).sort((a, b) => a.id.localeCompare(b.id))
  return (
    <div>
      <table className="ports data">
        <thead>
          <tr>
            <th>Rule</th>
            <th>Hardware</th>
            <th>Roles</th>
            <th>Template</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <RuleRow key={r.id} r={r} onEdit={() => setEditing({ existing: true, draft: { id: r.id, profile: r.profile, roles: r.roles, template: r.template ?? '' } })} />
          ))}
          {rows.length === 0 && (
            <tr>
              <td colSpan={5} className="muted">
                No rules; joining hosts wait for approval.
              </td>
            </tr>
          )}
        </tbody>
      </table>
      <div className="actions">
        <button onClick={() => setEditing({ existing: false, draft: { id: '', profile: 'Ms01', roles: ['Compute'], template: '' } })}>New rule</button>
      </div>
      {editing && <RuleEditorForm key={`${editing.existing}:${editing.draft.id}`} initial={editing.draft} existing={editing.existing} onDone={() => setEditing(null)} />}
    </div>
  )
}

export function NetworksPage() {
  const [tab, setTab] = useState<Tab>('Networks')
  const [change, setChange] = useState<Change | null>(null)
  const [showRollouts, setShowRollouts] = useState(false)
  const rollouts = useStore((s) => s.view.rollouts)
  const active = Object.values(rollouts).filter((r) => r.state !== 'Done' && r.state !== 'Aborted').length
  return (
    <section className="networks-page">
      <div className="fleet-bar">
        <div className="tabs inline-tabs" role="tablist">
          {TABS.map((t) => (
            <button key={t} role="tab" aria-selected={t === tab} className={t === tab ? 'active' : ''} onClick={() => setTab(t)}>
              {t}
            </button>
          ))}
        </div>
        <div className="spacer" />
        <button onClick={() => setShowRollouts(true)}>Rollouts{active > 0 ? ` (${active} active)` : ''}</button>
      </div>
      {tab === 'Networks' && <NetworksTab onRollout={setChange} />}
      {tab === 'Templates' && <TemplatesTab onRollout={setChange} />}
      {tab === 'Auto-approve rules' && <RulesTab />}
      {(change || showRollouts) && (
        <RolloutDialog
          key={change ? JSON.stringify(change) : 'list'}
          change={change}
          onClose={() => {
            setChange(null)
            setShowRollouts(false)
          }}
        />
      )}
    </section>
  )
}
