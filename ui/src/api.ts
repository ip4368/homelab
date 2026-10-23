import type { ApiOp } from './gen/ApiOp'
import type { Fault } from './gen/Fault'
import type { Scenario } from './gen/Scenario'

/** A scenario to run, or a preset to start the fleet over with. */
export type ScenarioRequest = Scenario | { preset: 'default' | 'scale500' }

/**
 * POSTs `body` as JSON. A non-2xx answer (400 rejected, 409 paused, 503 control
 * plane unavailable) rejects with the server's `{"error": ...}` string.
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

export async function fault(f: Fault): Promise<void> {
  await post('/api/sim/fault', f)
}

export async function scenario(s: ScenarioRequest): Promise<void> {
  await post('/api/sim/scenario', s)
}

/** 0 pauses; otherwise 1, 5, 20 or 60. */
export async function setSpeed(n: number): Promise<void> {
  await post('/api/sim/speed', { speed: n })
}
