import { describe, expect, it } from 'vitest'
import { closeSession, defaultContainer, emptyDock, logsSession, openSession, terminalSession } from './dock'
import { endingLine, execUrl, parseEnding, resizeMessage } from './terminal'
import { LineSplitter, appendLines, filterLines, logsUrl, parseLine } from './logs'

describe('logs', () => {
  it('cuts chunks into lines, a line spanning two chunks included', () => {
    const s = new LineSplitter()
    expect(s.push('one\ntw')).toEqual(['one'])
    expect(s.push('o\nthree\n')).toEqual(['two', 'three'])
    expect(s.push('four')).toEqual([])
    expect(s.flush()).toEqual(['four'])
    expect(s.flush()).toEqual([])
  })

  it("reads Kubernetes' timestamp and drops colour codes", () => {
    expect(parseLine('2026-11-04T15:14:02.553326968Z wrote 64 MiB')).toEqual({ at: '2026-11-04T15:14:02.553326968Z', text: 'wrote 64 MiB' })
    expect(parseLine('\x1b[32mINFO\x1b[0m ready\r')).toEqual({ at: null, text: 'INFO ready' })
  })

  it('keeps the newest lines and filters them ignoring case', () => {
    const l = (text: string) => ({ at: null, text })
    const lines = appendLines([l('a'), l('b')], [l('c'), l('D')], 3)
    expect(lines.map((x) => x.text)).toEqual(['b', 'c', 'D'])
    expect(filterLines(lines, ' d ').map((x) => x.text)).toEqual(['D'])
    expect(filterLines(lines, '')).toBe(lines)
  })

  it('asks the server for the right log', () => {
    const t = { namespace: 'demo', pod: 'web-1', container: 'nginx', previous: true }
    expect(logsUrl(t, { follow: true })).toBe('/api/kube/logs?namespace=demo&pod=web-1&container=nginx&previous=true&follow=true')
    expect(logsUrl({ ...t, container: null, previous: false }, { follow: false, tail: 0 })).toBe('/api/kube/logs?namespace=demo&pod=web-1&tail=0&follow=false')
  })
})

describe('dock', () => {
  const state = (name: string, role: 'App' | 'Init' | 'Sidecar') => ({ name, role, state: 'running', ready: true, restarts: 0, image_id: null })
  const pod = { namespace: 'demo', name: 'web-1', container_states: [state('migrate', 'Init'), state('nginx', 'App'), state('exporter', 'App')] }

  it("opens a pod's log on its first app container", () => {
    expect(defaultContainer(pod)).toBe('nginx')
    expect(defaultContainer({ container_states: [] })).toBeNull()
    expect(logsSession(pod).title).toBe('web-1/nginx')
    expect(logsSession(pod, 'nginx', true).title).toBe('web-1/nginx (previous)')
    expect(logsSession({ ...pod, container_states: [state('nginx', 'App')] }).title).toBe('web-1')
  })

  it('shows a session already open instead of opening it twice, and closing shows a neighbour', () => {
    const a = logsSession(pod)
    const b = logsSession(pod, 'exporter')
    let s = openSession(openSession(emptyDock, a), b)
    expect([s.sessions.length, s.active]).toEqual([2, b.id])
    s = openSession(s, logsSession(pod))
    expect([s.sessions.length, s.active]).toEqual([2, a.id])
    s = closeSession(s, a.id)
    expect([s.sessions.map((x) => x.id), s.active]).toEqual([[b.id], b.id])
    expect(closeSession(s, b.id)).toEqual(emptyDock)
  })
})

describe('terminal', () => {
  it('opens a shell on the page\'s own server', () => {
    const t = { namespace: 'demo', pod: 'web-1', container: 'nginx' }
    expect(execUrl(t, { protocol: 'http:', host: 'localhost:8080' })).toBe('ws://localhost:8080/api/kube/exec?namespace=demo&pod=web-1&container=nginx')
    expect(execUrl({ ...t, container: null }, { protocol: 'https:', host: 'lab' })).toBe('wss://lab/api/kube/exec?namespace=demo&pod=web-1')
    expect(resizeMessage(120, 40)).toBe('{"resize":{"cols":120,"rows":40}}')
  })

  it('says how a session ended', () => {
    expect(parseEnding('{"exit":{"code":3}}')).toEqual({ exit: 3 })
    expect(parseEnding('{"error":"no shell"}')).toEqual({ error: 'no shell' })
    expect(parseEnding('hello')).toBeNull()
    expect([endingLine({ exit: 0 }), endingLine({ exit: 3 }), endingLine({ error: 'no shell' }), endingLine(null)]).toEqual([
      'session ended',
      'session ended with exit code 3',
      'no shell',
      'connection closed',
    ])
  })

  it('gives every shell its own tab', () => {
    const pod = { namespace: 'demo', name: 'web-1', container_states: [] }
    expect(terminalSession(pod).id).not.toBe(terminalSession(pod).id)
  })
})
