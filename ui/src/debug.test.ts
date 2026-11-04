import { describe, expect, it } from 'vitest'
import { appendLog, consoleUrl, execUrl, filterLog, journalUrl, labTerminalUrl, logFollowUrl, logParams, resizeFrame, terminalNotice, socketUrl } from './debug'

const http = { protocol: 'http:', host: 'lab:8080' }
const https = { protocol: 'https:', host: 'lab' }

describe('request building', () => {
  it('builds the log query the server reads', () => {
    expect(logParams({ namespace: 'demo', pod: 'web-1' })).toBe('namespace=demo&pod=web-1')
    expect(logParams({ namespace: 'demo', pod: 'web-1', container: 'app', tail: 500, previous: true, timestamps: true, sinceSeconds: 60 })).toBe(
      'namespace=demo&pod=web-1&container=app&tail=500&previous=true&since_seconds=60&timestamps=true',
    )
    expect(logParams({ namespace: 'demo', pod: 'web-1', previous: false, timestamps: false })).toBe('namespace=demo&pod=web-1')
  })

  it('builds websocket urls on the page host', () => {
    expect(socketUrl('/api/ws', http)).toBe('ws://lab:8080/api/ws')
    expect(socketUrl('/api/ws', https)).toBe('wss://lab/api/ws')
    expect(logFollowUrl({ namespace: 'demo', pod: 'w riter' }, http)).toBe('ws://lab:8080/api/kube/logs/ws?namespace=demo&pod=w+riter')
    expect(execUrl({ namespace: 'demo', pod: 'web-1', container: 'app', shell: '/bin/bash' }, http)).toBe(
      'ws://lab:8080/api/kube/exec/ws?namespace=demo&pod=web-1&container=app&shell=%2Fbin%2Fbash',
    )
    expect(execUrl({ namespace: 'demo', pod: 'web-1' }, http)).toBe('ws://lab:8080/api/kube/exec/ws?namespace=demo&pod=web-1')
    expect(labTerminalUrl('ms01-01', http)).toBe('ws://lab:8080/api/lab/terminal/ws?vm=ms01-01')
    expect(journalUrl('ms01-01', 'homelabd', 100)).toBe('/api/lab/journal?vm=ms01-01&unit=homelabd&lines=100')
    expect(journalUrl('ms01-01', 'k3s')).toBe('/api/lab/journal?vm=ms01-01&unit=k3s')
    expect(consoleUrl('ms01-01')).toBe('/api/lab/console?vm=ms01-01')
  })
})

describe('terminal frames', () => {
  it('reads errors and endings, and nothing else', () => {
    expect(terminalNotice('{"error":"no shell"}')).toEqual({ kind: 'error', message: 'no shell' })
    expect(terminalNotice('{"status":"the command exited"}')).toEqual({ kind: 'status', message: 'the command exited' })
    expect(terminalNotice('plain output')).toBeNull()
    expect(terminalNotice('{"resize":{}}')).toBeNull()
    expect(terminalNotice('{"error":3}')).toBeNull()
    expect(terminalNotice('{broken')).toBeNull()
  })

  it('writes the resize frame the bridges parse', () => {
    expect(JSON.parse(resizeFrame(120, 40))).toEqual({ resize: { width: 120, height: 40 } })
  })
})

describe('followed logs', () => {
  it('caps on a line boundary and filters case-insensitively', () => {
    expect(appendLog('one\n', 'two\n', 100)).toBe('one\ntwo\n')
    expect(appendLog('one\ntwo\n', 'three\n', 9)).toBe('three\n')
    expect(appendLog('', 'no lines at all here', 5)).toBe(' here')
    expect(filterLog('Error: boom\nall fine\nanother ERROR\n', 'error')).toBe('Error: boom\nanother ERROR')
    expect(filterLog('a\nb', '  ')).toBe('a\nb')
    expect(filterLog('a\nb', 'zzz')).toBe('')
  })
})
