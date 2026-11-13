import { useEffect } from 'react'
import App from './App'
import { auth, useAuth } from './auth'
import { LoginDialog, LoginPage } from './components/Login'
import { store } from './store'
import { connect } from './ws'

/**
 * The login gate: the login page until someone logs in, then the workspace
 * and its live view. If the session ends while it is open, a login dialog
 * covers it instead, and the live view reconnects after the new login.
 */
export function Root() {
  const state = useAuth()
  const live = state.phase === 'in' ? state.epoch : null

  useEffect(() => {
    void auth.refresh()
  }, [])

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
  if (state.phase === 'anonymous') return <LoginPage methods={state.methods} />
  return (
    <>
      <App />
      {state.phase === 'lost' && <LoginDialog methods={state.methods} user={state.user} />}
    </>
  )
}
