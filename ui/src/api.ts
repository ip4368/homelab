import type { ApiOp } from './gen/ApiOp'
import type { LabFault } from './gen/LabFault'

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

/** Does something to the lab's hardware; resolves with the new machine's id for `AddVm`. */
export async function labFault(f: LabFault): Promise<string | null> {
  const result = (await post('/api/lab/fault', f)) as { vm?: string }
  return result.vm ?? null
}
