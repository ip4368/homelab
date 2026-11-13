import { useState, type FormEvent } from 'react'
import { auth } from '../auth'
import type { AuthMethod } from '../gen/AuthMethod'

/**
 * The login form, for each way the server offers (only a password today; an
 * OIDC provider would add a "Log in with …" button here). `user` fills in
 * the name when a session ended under a running UI.
 */
export function LoginForm({ methods, user, autoFocus = true }: { methods: AuthMethod[]; user?: string; autoFocus?: boolean }) {
  const [username, setUsername] = useState(user ?? '')
  const [password, setPassword] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  const submit = async (e: FormEvent) => {
    e.preventDefault()
    setBusy(true)
    setError(null)
    try {
      await auth.login(username.trim(), password)
    } catch (err) {
      setError(String(err))
      setPassword('')
    } finally {
      setBusy(false)
    }
  }

  if (!methods.some((m) => m.kind === 'password')) return <p className="muted">This server offers no way to log in here.</p>
  return (
    <form className="login-form" onSubmit={submit}>
      <label>
        <span>User name</span>
        <input name="username" autoComplete="username" value={username} onChange={(e) => setUsername(e.target.value)} autoFocus={autoFocus && !user} required />
      </label>
      <label>
        <span>Password</span>
        <input name="password" type="password" autoComplete="current-password" value={password} onChange={(e) => setPassword(e.target.value)} autoFocus={autoFocus && !!user} required />
      </label>
      {error && (
        <div className="login-error" role="alert">
          {error}
        </div>
      )}
      <button type="submit" className="active" disabled={busy || !username.trim() || !password}>
        {busy ? 'Logging in…' : 'Log in'}
      </button>
    </form>
  )
}

/** The whole page, when nobody is logged in. */
export function LoginPage({ methods }: { methods: AuthMethod[] }) {
  return (
    <div className="login-page">
      <div className="login-card">
        <div className="nav-brand">
          <span className="brand-mark">H</span>
          <span className="brand-copy">
            <strong>Homelab</strong>
            <small>operations</small>
          </span>
        </div>
        <p className="muted">Log in to manage the cluster: its hosts, network, workloads and lab.</p>
        <LoginForm methods={methods} />
      </div>
    </div>
  )
}

/** Over the workspace, when the session ended while it was open: nothing on screen is lost. */
export function LoginDialog({ methods, user }: { methods: AuthMethod[]; user: string }) {
  return (
    <div className="modal-backdrop login-backdrop">
      <div className="modal login-dialog" role="dialog" aria-modal="true" aria-label="log in again">
        <h3>Your session ended</h3>
        <p className="muted">
          It expired, was logged out elsewhere, or {user}'s password changed. Log in again to carry on; what is on screen stays as it
          was.
        </p>
        <LoginForm methods={methods} user={user} />
        <button className="login-switch" onClick={() => void auth.logout()}>
          Log in as someone else
        </button>
      </div>
    </div>
  )
}
