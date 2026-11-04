import { useState } from 'react'
import { kubeApply } from '../api'
import type { ApplyReport } from '../gen/ApplyReport'
import { outcomeLabel, previewVerdict } from '../kube'
import { NewAppForm } from './NewAppForm'
import { NewVmForm } from './NewVmForm'
import { useAction } from './useAction'

export interface ApplyDraft {
  title: string
  yaml: string
  /** For namespaced objects that name none. */
  namespace: string
  /** Start with "take over" ticked (an edit of a live object, as `kubectl edit`). */
  force?: boolean
  /** Shown under the title. */
  note?: string
  /** Offer the New from image form beside the YAML. */
  offerNew?: boolean
}

const PLACEHOLDER = `apiVersion: apps/v1
kind: Deployment
metadata:
  name: hello
spec:
  replicas: 2
  selector:
    matchLabels: { app: hello }
  template:
    metadata:
      labels: { app: hello }
    spec:
      containers:
        - name: hello
          image: nginx:1.29`

function Report({ report }: { report: ApplyReport }) {
  return (
    <table className="data apply-report">
      <tbody>
        {report.objects.map((o) => {
          const { text, tone } = outcomeLabel(o.outcome, report.dry_run)
          return (
            <tr key={`${o.kind}/${o.namespace ?? ''}/${o.name}`}>
              <td>{o.kind}</td>
              <td>
                {o.namespace && <span className="muted">{o.namespace}/</span>}
                <strong>{o.name}</strong>
              </td>
              <td className={tone}>{text}</td>
            </tr>
          )
        })}
      </tbody>
    </table>
  )
}

/**
 * Applies YAML as `kubectl apply --server-side` would: Preview checks it with
 * the API's dry run, and Apply is offered once a preview of exactly this YAML
 * found no problem. Fields another manager owns are taken over only when asked.
 */
export function ApplyDialog({ draft, namespaces, onClose }: { draft: ApplyDraft; namespaces: string[]; onClose(): void }) {
  const [yaml, setYaml] = useState(draft.yaml)
  const [namespace, setNamespace] = useState(draft.namespace)
  const [force, setForce] = useState(draft.force ?? false)
  const [tab, setTab] = useState<'yaml' | 'new' | 'vm'>('yaml')
  const [preview, setPreview] = useState<{ report: ApplyReport; yaml: string; namespace: string; force: boolean } | null>(null)
  const [applied, setApplied] = useState<ApplyReport | null>(null)
  const previewing = useAction()
  const applying = useAction()
  const busy = previewing.busy || applying.busy

  const current = preview !== null && preview.yaml === yaml && preview.namespace === namespace && preview.force === force
  const verdict = preview ? previewVerdict(preview.report) : null
  const run = (dry_run: boolean) => kubeApply({ yaml, namespace, dry_run, force })
  const check = () =>
    previewing.run(async () => {
      setApplied(null)
      setPreview({ report: await run(true), yaml, namespace, force })
    })
  const apply = () =>
    applying.run(async () => {
      setApplied(await run(false))
      // Applying again needs another preview (of whatever changed since).
      setPreview(null)
    })

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal apply-dialog" role="dialog" aria-label="apply yaml" onClick={(e) => e.stopPropagation()}>
        <div className="drawer-head">
          <h3>{draft.title}</h3>
          <div className="spacer" />
          <button onClick={onClose} aria-label="close">
            ×
          </button>
        </div>
        {draft.note && <p className="muted">{draft.note}</p>}
        {draft.offerNew && (
          <div className="tabs inline-tabs" role="tablist">
            <button role="tab" aria-selected={tab === 'yaml'} className={tab === 'yaml' ? 'active' : ''} onClick={() => setTab('yaml')}>
              YAML
            </button>
            <button role="tab" aria-selected={tab === 'new'} className={tab === 'new' ? 'active' : ''} onClick={() => setTab('new')} disabled={busy}>
              New from image
            </button>
            <button role="tab" aria-selected={tab === 'vm'} className={tab === 'vm' ? 'active' : ''} onClick={() => setTab('vm')} disabled={busy}>
              New VM
            </button>
          </div>
        )}
        {tab === 'vm' && (
          <NewVmForm
            namespace={namespace}
            namespaces={namespaces}
            onWrite={(y, ns) => {
              setYaml(y)
              setNamespace(ns)
              setTab('yaml')
            }}
          />
        )}
        {tab === 'new' && (
          <NewAppForm
            namespace={namespace}
            namespaces={namespaces}
            onWrite={(y, ns) => {
              setYaml(y)
              setNamespace(ns)
              setTab('yaml')
            }}
          />
        )}
        <div hidden={tab !== 'yaml'}>
          <textarea
            className="yaml-box"
            value={yaml}
            onChange={(e) => setYaml(e.target.value)}
            placeholder={PLACEHOLDER}
            spellCheck={false}
            aria-label="manifests"
            disabled={busy}
          />
          <div className="actions">
            <label className="inline">
              Namespace{' '}
              <input value={namespace} onChange={(e) => setNamespace(e.target.value.trim())} placeholder="default" list="apply-namespaces" size={16} aria-label="default namespace" disabled={busy} />
            </label>
            <datalist id="apply-namespaces">
              {namespaces.map((n) => (
                <option key={n} value={n} />
              ))}
            </datalist>
            <span className="muted">for objects that name none</span>
            <div className="spacer" />
            {(force || (verdict?.conflicts.length ?? 0) > 0) && (
              <label className="inline warn" title="Apply with --force-conflicts: fields other managers own become homelab-ui's">
                <input type="checkbox" checked={force} onChange={(e) => setForce(e.target.checked)} disabled={busy} /> take over
              </label>
            )}
            <button onClick={check} disabled={busy || yaml.trim() === ''}>
              Preview
            </button>
            <button className="active" onClick={apply} disabled={busy || !current || !verdict?.canApply} title={current ? undefined : 'Preview this YAML first'}>
              Apply
            </button>
          </div>
          {previewing.error && (
            <div className="bad refusal" role="alert">
              {previewing.error}
            </div>
          )}
          {preview && !applied && (
            <>
              <h4 className="modal-sub">{current ? 'Preview' : 'Preview (of an earlier version: preview again)'}</h4>
              <Report report={preview.report} />
              {verdict && verdict.conflicts.length > 0 && (
                <div className="warn refusal">
                  Other field managers own fields this YAML sets to other values:
                  <ul className="fixes">
                    {verdict.conflicts.map((c) => (
                      <li key={`${c.object}${c.field}`}>
                        {c.object}: <code>{c.field}</code> (owned by {c.manager})
                      </li>
                    ))}
                  </ul>
                  Tick <em>take over</em> and preview again to apply anyway.
                </div>
              )}
            </>
          )}
          {applying.error && (
            <div className="bad refusal" role="alert">
              {applying.error}
            </div>
          )}
          {applied && (
            <>
              <h4 className="modal-sub">Applied</h4>
              <Report report={applied} />
            </>
          )}
        </div>
      </div>
    </div>
  )
}
