import { describe, expect, it } from 'vitest'
import { formatRoute, HOME, parseRoute, type Route } from './route'

describe('routes in the URL hash', () => {
  it('round-trips a page, its list, the open detail and its tab', () => {
    const routes: Route[] = [
      HOME,
      { page: 'hosts', list: null, open: { kind: 'host', id: 'ms01-01' }, tab: 'network' },
      { page: 'workloads', list: 'pods', open: { kind: 'workload', id: 'demo/Deployment/web' }, tab: null },
      { page: 'topology', list: null, open: { kind: 'switch', id: 'access-1' }, tab: null },
      { page: 'networks', list: 'templates', open: null, tab: null },
    ]
    for (const r of routes) expect(parseRoute(formatRoute(r))).toEqual(r)
    expect(formatRoute(routes[2])).toBe('#/workloads/pods?open=workload%3Ademo%2FDeployment%2Fweb')
  })

  it('opens the old tabbed UI links on the matching page', () => {
    expect(parseRoute('#fleet').page).toBe('hosts')
    expect(parseRoute('#topology').page).toBe('topology')
    expect(parseRoute('#workloads').page).toBe('workloads')
    expect(parseRoute('').page).toBe('overview')
  })

  it('falls back to the overview, or no detail, on what it does not know', () => {
    expect(parseRoute('#/nowhere')).toEqual(HOME)
    expect(parseRoute('#/hosts?open=pod:x').open).toBeNull()
    expect(parseRoute('#/hosts?open=host:').open).toBeNull()
    // A tab without anything open means nothing.
    expect(parseRoute('#/hosts?tab=network').tab).toBeNull()
  })
})
