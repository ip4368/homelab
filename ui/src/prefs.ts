/**
 * Layout choices the UI remembers in the browser: whether the activity panel
 * and the side navigation are open, the detail pane's width, the hosts view.
 */
import { useSyncExternalStore } from 'react'

const PREFIX = 'homelab.'
const listeners = new Set<() => void>()
const memory = new Map<string, unknown>()

function read<T>(key: string, fallback: T): T {
  if (memory.has(key)) return memory.get(key) as T
  let value = fallback
  try {
    const raw = localStorage.getItem(PREFIX + key)
    if (raw !== null) value = JSON.parse(raw) as T
  } catch {
    // No storage (a private window), or something else wrote it: the default will do.
  }
  memory.set(key, value)
  return value
}

export function setPref<T>(key: string, value: T) {
  memory.set(key, value)
  try {
    localStorage.setItem(PREFIX + key, JSON.stringify(value))
  } catch {
    // Kept for this page only.
  }
  listeners.forEach((l) => l())
}

export function usePref<T>(key: string, fallback: T): [T, (v: T) => void] {
  const value = useSyncExternalStore(
    (l) => {
      listeners.add(l)
      return () => listeners.delete(l)
    },
    () => read(key, fallback),
  )
  return [value, (v: T) => setPref(key, v)]
}
