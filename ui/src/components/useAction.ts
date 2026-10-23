import { useCallback, useState } from 'react'

/** Runs one user action at a time and keeps its error, so the caller can show it next to the control that triggered it. */
export function useAction() {
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const run = useCallback(async (action: () => Promise<unknown>) => {
    setError(null)
    setBusy(true)
    try {
      await action()
    } catch (e) {
      setError(String(e))
    } finally {
      setBusy(false)
    }
  }, [])
  return { error, busy, run }
}
