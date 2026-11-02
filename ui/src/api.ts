import type { ApiOp } from './gen/ApiOp'
import type { ApplyReport } from './gen/ApplyReport'
import type { ApplyRequest } from './gen/ApplyRequest'
import type { KubeAction } from './gen/KubeAction'
import type { LabFault } from './gen/LabFault'
import type { WorkloadRef } from './gen/WorkloadRef'

/**
 * POSTs `body` as JSON. A non-2xx answer (400 rejected, 503 control plane or
 * lab unavailable) rejects with the server's `{"error": ...}` string.
 */
async function post(path: string, body: unknown): Promise<unknown> {
  const res = await fetch(path, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  })
  const json: unknown = await res.json().catch(() => null)
  if (!res.ok) {
    const error = (json as { error?: unknown } | null)?.error
    throw typeof error === 'string' ? error : `HTTP ${res.status}`
  }
  return json
}

/** Submits a user operation; resolves with the `Ok` payload (e.g. the token for `CreateJoinToken`). */
export async function op(o: ApiOp): Promise<unknown> {
  const result = (await post('/api/ops', o)) as { Ok: unknown }
  return result.Ok
}

/** Applies the demo workloads (deploy/demo-workloads.yaml) to the cluster's Kubernetes; resolves with kubectl's output. */
export async function seedWorkloads(): Promise<string> {
  const result = (await post('/api/lab/seed', {})) as { output: string }
  return result.output
}

/** Scales, restarts or deletes a workload in the cluster's Kubernetes, as kubectl would. */
export async function kubeAction(a: KubeAction): Promise<void> {
  await post('/api/kube', a)
}

/** Applies manifests (or with `dry_run`, only checks them) as `kubectl apply --server-side` would; what became of each object. */
export async function kubeApply(req: ApplyRequest): Promise<ApplyReport> {
  return (await post('/api/kube/apply', req)) as ApplyReport
}

/** A live workload as YAML, to edit and apply again. */
export async function kubeManifest(w: WorkloadRef): Promise<string> {
  const res = await fetch(`/api/kube/manifest?${new URLSearchParams({ namespace: w.namespace, kind: w.kind, name: w.name })}`)
  if (res.ok) return res.text()
  const error = ((await res.json().catch(() => null)) as { error?: unknown } | null)?.error
  throw typeof error === 'string' ? error : `HTTP ${res.status}`
}

/** Does something to the lab's hardware; resolves with the new machine's id for `AddVm`. */
export async function labFault(f: LabFault): Promise<string | null> {
  const result = (await post('/api/lab/fault', f)) as { vm?: string }
  return result.vm ?? null
}
