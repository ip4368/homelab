import { useEffect } from 'react'
import App from './App'
import { auth, useAuth } from './auth'
import { LoginDialog, LoginPage } from './components/Login'
import { store } from './store'
import { connect } from './ws'

/** How often to ask again while the server cannot be reached on first load. */
const RETRY_MS = 5000

/**
 * The login gate: the login page until someone logs in, then the workspace
 * and its live view. If the session ends while it is open, a login dialog
 * covers it instead, and the live view reconnects after the new login. If
 * the server cannot be reached on first load, it says so and keeps asking.
 */
export function Root() {
  const state = useAuth()
  const live = state.phase === 'in' ? state.epoch : null

  useEffect(() => {
    void auth.refresh()
  }, [])

  const unreachable = state.phase === 'unreachable'
  useEffect(() => {
    if (!unreachable) return
    const timer = setInterval(() => void auth.refresh(), RETRY_MS)
    return () => clearInterval(timer)
  }, [unreachable])

  useEffect(() => {
    if (live === null) return
    return connect(store, undefined, {
      onSessionEnded: () => auth.expired(),
      stillLoggedIn: async () => {
        const status = await auth.refresh()
        return status === null ? undefined : status.session !== null
      },
    })
  }, [live])

  if (state.phase === 'checking') return <div className="login-page muted">connecting…</div>
  if (state.phase === 'unreachable')
    return (
      <div className="login-page">
        <div className="login-card" role="alert">
          <p>
            <strong>The homelab server is not answering</strong>
          </p>
          <p className="muted">
            {state.reason}. Trying again every {RETRY_MS / 1000} s; if it is restarting this clears by itself.
          </p>
          <button className="active" onClick={() => void auth.refresh()}>
            Try now
          </button>
        </div>
      </div>
    )
  if (state.phase === 'anonymous') return <LoginPage methods={state.methods} />
  return (
    <>
      <App />
      {state.phase === 'lost' && <LoginDialog methods={state.methods} user={state.user} />}
    </>
  )
}
