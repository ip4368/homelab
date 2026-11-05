import { useEffect, useMemo, useState } from 'react'
import { kubeAction, kubeManifest } from '../api'
import type { KubeWorkload } from '../gen/KubeWorkload'
import { CONSOLE_LOG_CONTAINER, consoleSession, dock, logsSession, terminalSession } from '../dock'
import { canShell, deletePrompt, editDraft, restartPrompt, stopPrompt, workloadActions, workloadClaims, workloadTone } from '../kube'
import { useStore } from '../store'
import { ApplyDialog, type ApplyDraft } from './ApplyDialog'
import { Icon } from './Icons'
import { Menu, type MenuItem } from './Menu'
import { useAction } from './useAction'

/**
 * `ready/desired`, with − and + to scale where the workload scales: the new
 * count waits for Scale (or Enter) so a stray click changes nothing.
 */
export function ScaleControl({ w, always = false }: { w: KubeWorkload; always?: boolean }) {
  const can = workloadActions(w)
  const { error, busy, run } = useAction()
  const [wanted, setWanted] = useState<number | null>(null)
  // Kubernetes reports the asked-for count: the edit is done.
  useEffect(() => {
    if (wanted !== null && wanted === w.desired && !busy) setWanted(null)
  }, [w.desired, wanted, busy])
  const shown = wanted ?? w.desired
  const workload = { namespace: w.namespace, kind: w.kind, name: w.name }
  const scale = () => {
    if (wanted === null || wanted === w.desired) return
    void run(() =>
      kubeAction({ Scale: { workload, replicas: wanted } }).catch((e: unknown) => {
        setWanted(null)
        throw e
      }),
    )
  }
  return (
    <span className={`scale${always ? ' always' : ''}`} onClick={(e) => e.stopPropagation()}>
      <span className={`num ${workloadTone(w)}`}>
        {w.ready}/{shown}
      </span>
      {can.scale && (
        <span className="stepper">
          <button className="small" aria-label={`fewer replicas of ${w.name}`} disabled={busy || shown === 0} onClick={() => setWanted(Math.max(0, shown - 1))}>
            −
          </button>
          <button className="small" aria-label={`more replicas of ${w.name}`} disabled={busy} onClick={() => setWanted(shown + 1)}>
            +
          </button>
        </span>
      )}
      {wanted !== null && wanted !== w.desired && (
        <>
          <button className="small primary" disabled={busy} onClick={scale} onKeyDown={(e) => e.key === 'Escape' && setWanted(null)} autoFocus>
            Scale to {wanted}
          </button>
          <button className="link" disabled={busy} onClick={() => setWanted(null)}>
            cancel
          </button>
        </>
      )}
      {error && (
        <span className="bad refusal" role="alert">
          {error}
        </span>
      )}
    </span>
  )
}

/** Start, Stop, Restart, Edit YAML and Delete as menu items, each asking first where it should. */
function useWorkloadItems(w: KubeWorkload) {
  const pods = useStore((s) => s.kube.pods)
  const volumes = useStore((s) => s.kube.volumes)
  const { error, busy, run } = useAction()
  const [draft, setDraft] = useState<ApplyDraft | null>(null)
  const can = workloadActions(w)
  const workload = { namespace: w.namespace, kind: w.kind, name: w.name }
  const items: Record<'start' | 'stop' | 'restart' | 'edit' | 'remove', MenuItem | null> = {
    start: can.start ? { label: 'Start', disabled: busy, onClick: () => run(() => kubeAction({ Start: { workload } })) } : null,
    stop: can.stop ? { label: 'Stop', disabled: busy, onClick: () => window.confirm(stopPrompt(w)) && run(() => kubeAction({ Stop: { workload } })) } : null,
    restart: can.restart ? { label: 'Restart', disabled: busy, onClick: () => window.confirm(restartPrompt(w)) && run(() => kubeAction({ Restart: { workload } })) } : null,
    edit: can.edit ? { label: 'Edit YAML', disabled: busy, onClick: () => run(async () => setDraft(editDraft(w, await kubeManifest(workload)))) } : null,
    remove: can.remove
      ? { label: 'Delete…', danger: true, disabled: busy, onClick: () => window.confirm(deletePrompt(w, workloadClaims(w, pods, volumes))) && run(() => kubeAction({ Delete: { workload } })) }
      : null,
  }
  const namespaces = useMemo(() => [...new Set(pods.map((p) => p.namespace))].sort(), [pods])
  const dialog = draft && <ApplyDialog draft={draft} namespaces={namespaces} onClose={() => setDraft(null)} />
  return { items, error, busy, dialog, system: !can.remove }
}

/** A workload row's actions: Logs of its first pod, and the rest behind "⋯". */
export function WorkloadRowActions({ w }: { w: KubeWorkload }) {
  const pods = useStore((s) => s.kube.pods)
  const first = useMemo(() => pods.find((p) => p.namespace === w.namespace && p.owner === `${w.kind}/${w.name}`), [pods, w])
  const { items, error, dialog, system } = useWorkloadItems(w)
  return (
    <span className="row-buttons" onClick={(e) => e.stopPropagation()}>
      {first && (
        <button className="small" title={`Follow ${first.name}'s log`} aria-label={`logs of ${first.name}`} onClick={() => dock.open(w.vm ? logsSession(first, CONSOLE_LOG_CONTAINER) : logsSession(first))}>
          <Icon name="logs" size={14} /> <span className="btn-label">Logs</span>
        </button>
      )}
      {system ? (
        <span className="pill muted" title="K3s, Longhorn and KubeVirt run here, installed by homelabd: change it with kubectl">
          system
        </span>
      ) : (
        <Menu label={`actions for ${w.name}`} items={[items.start, items.stop, items.restart, items.edit, items.remove]} />
      )}
      {error && (
        <span className="bad refusal" role="alert">
          {error}
        </span>
      )}
      {dialog}
    </span>
  )
}

/** The workload pane's toolbar: logs, shell or console, then its changes, Delete last behind "⋯". */
export function WorkloadPaneActions({ w }: { w: KubeWorkload }) {
  const pods = useStore((s) => s.kube.pods)
  const first = useMemo(() => pods.find((p) => p.namespace === w.namespace && p.owner === `${w.kind}/${w.name}`), [pods, w])
  const { items, error, dialog, system } = useWorkloadItems(w)
  const button = (i: MenuItem | null, icon?: Parameters<typeof Icon>[0]['name']) =>
    i && (
      <button disabled={i.disabled} onClick={i.onClick}>
        {icon && <Icon name={icon} size={14} />} {i.label}
      </button>
    )
  return (
    <>
      {w.vm ? (
        <>
          <button disabled={w.ready === 0} onClick={() => dock.open(consoleSession(w.namespace, w.name))} title="Its serial console, as virtctl console">
            <Icon name="console" size={14} /> Console
          </button>
          {first && (
            <button onClick={() => dock.open(logsSession(first, CONSOLE_LOG_CONTAINER))} title="What its serial console printed, even with nobody attached">
              <Icon name="logs" size={14} /> Console log
            </button>
          )}
        </>
      ) : (
        first && (
          <>
            <button onClick={() => dock.open(logsSession(first))} title={`Follow ${first.name}'s log`}>
              <Icon name="logs" size={14} /> Logs
            </button>
            {canShell(first) && (
              <button onClick={() => dock.open(terminalSession(first))} title={`A shell in ${first.name}`}>
                <Icon name="terminal" size={14} /> Shell
              </button>
            )}
          </>
        )
      )}
      <span className="toolbar-sep" />
      {button(items.start, 'play')}
      {button(items.stop, 'stop')}
      {button(items.restart, 'restart')}
      {button(items.edit, 'edit')}
      {items.remove && <Menu label={`more actions for ${w.name}`} items={[items.remove]} />}
      {system && (
        <span className="muted" title="K3s, Longhorn and KubeVirt run here, installed by homelabd: change it with kubectl">
          system workload: read only here
        </span>
      )}
      {error && (
        <span className="bad refusal" role="alert">
          {error}
        </span>
      )}
      {dialog}
    </>
  )
}
