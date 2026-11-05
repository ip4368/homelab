import { describe, expect, it } from 'vitest'
import type { Alert } from './gen/Alert'
import type { HostObj } from './gen/HostObj'
import type { KubeWorkload } from './gen/KubeWorkload'
import type { LonghornVolume } from './gen/LonghornVolume'
import { attention } from './attention'

const host = (id: string, lifecycle: HostObj['lifecycle']) => ({ id, lifecycle, profile: { id: 'Ms01' } }) as unknown as HostObj
const alert = (id: string, severity: Alert['severity'], entity: Alert['entity'], resolved_at: number | null = null) =>
  ({ id, severity, kind: 'HostDown', entity, message: 'm', raised_at: 1, resolved_at, version: 1 }) as Alert
const workload = (name: string, state: KubeWorkload['health']['state']) =>
  ({ namespace: 'demo', kind: 'Deployment', name, ready: 0, desired: 1, health: { state, reason: state === 'Down' ? 'CrashLoopBackOff' : null } }) as KubeWorkload
const volume = (name: string, robustness: string) => ({ name, pvc: `demo/${name}`, robustness, state: 'attached', replicas: [], replicas_wanted: 2 }) as unknown as LonghornVolume

describe('what needs attention', () => {
  it('lists open alerts, hosts down or pending, unhealthy workloads and volumes, worst first', () => {
    const hosts = { a: host('a', 'Ready'), b: host('b', 'NotReady'), c: host('c', 'Pending') }
    const alerts = {
      1: alert('1', 'Warning', { kind: 'Switch', id: 'access-1' }),
      2: alert('2', 'Critical', { kind: 'Host', id: 'x' }, 5),
      3: alert('3', 'Info', { kind: 'Cp', id: 'cp' }),
    }
    const kube = { workloads: [workload('ok', 'Healthy'), workload('idle', 'Idle'), workload('web', 'Down'), workload('db', 'RollingOut')], volumes: [volume('v1', 'healthy'), volume('v2', 'degraded')] }
    const items = attention(hosts, alerts, kube)
    expect(items.map((i) => [i.key, i.tone])).toEqual([
      ['host:b', 'bad'],
      ['workload:demo/Deployment/web', 'bad'],
      ['alert:1', 'warn'],
      ['join:c', 'warn'],
      ['workload:demo/Deployment/db', 'warn'],
      ['volume:v2', 'warn'],
    ])
    expect(items[2].open).toEqual({ kind: 'switch', id: 'access-1' })
    expect(items[1].detail).toBe('Deployment down: CrashLoopBackOff (0/1 ready)')
  })

  it('does not list a host twice when an alert already names it', () => {
    const items = attention({ b: host('b', 'NotReady') }, { 1: alert('1', 'Critical', { kind: 'Host', id: 'b' }) }, { workloads: [], volumes: [] })
    expect(items.map((i) => i.key)).toEqual(['alert:1'])
  })
})
