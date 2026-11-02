import { describe, expect, it } from 'vitest'
import type { Role } from './gen/Role'
import { BLANK_APP, hostsWithRoles, newAppProblems, newAppYaml, type NewApp } from './newapp'

const web: NewApp = { ...BLANK_APP, name: 'web', image: 'nginx:1.29' }

describe('new from image', () => {
  it('a bare image is a Deployment that leaves a dead host after 20 s', () => {
    expect(newAppProblems(web)).toEqual([])
    expect(newAppYaml(web, false)).toBe(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels: { app: web }
  template:
    metadata:
      labels: { app: web }
    spec:
      tolerations:
        - { key: node.kubernetes.io/unreachable, operator: Exists, effect: NoExecute, tolerationSeconds: 20 }
        - { key: node.kubernetes.io/not-ready, operator: Exists, effect: NoExecute, tolerationSeconds: 20 }
      containers:
        - name: web
          image: "nginx:1.29"
`)
  })

  it('a volume makes a StatefulSet on the storage network, with a Service and an Ingress for its port and path', () => {
    const app: NewApp = { ...web, namespace: 'shop', replicas: '2', port: '8080', ingressPath: '/shop', volumeGi: '5', mountPath: '/var/lib/shop', roles: ['Compute'] }
    expect(newAppProblems(app)).toEqual([])
    expect(newAppYaml(app, true)).toBe(`apiVersion: v1
kind: Namespace
metadata:
  name: shop
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: web
  namespace: shop
spec:
  serviceName: web
  replicas: 2
  selector:
    matchLabels: { app: web }
  template:
    metadata:
      labels: { app: web }
    spec:
      nodeSelector: { "homelab.io/storage-network": "true", "homelab.io/compute": "true" }
      tolerations:
        - { key: node.kubernetes.io/unreachable, operator: Exists, effect: NoExecute, tolerationSeconds: 20 }
        - { key: node.kubernetes.io/not-ready, operator: Exists, effect: NoExecute, tolerationSeconds: 20 }
      containers:
        - name: web
          image: "nginx:1.29"
          ports: [{ containerPort: 8080 }]
          volumeMounts: [{ name: data, mountPath: "/var/lib/shop" }]
  volumeClaimTemplates:
    - metadata: { name: data }
      spec:
        accessModes: [ReadWriteOnce]
        storageClassName: longhorn
        resources: { requests: { storage: 5Gi } }
---
apiVersion: v1
kind: Service
metadata:
  name: web
  namespace: shop
spec:
  selector: { app: web }
  ports: [{ port: 8080 }]
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: web
  namespace: shop
spec:
  rules:
    - http:
        paths:
          - path: "/shop"
            pathType: Prefix
            backend: { service: { name: web, port: { number: 8080 } } }
`)
  })

  it('roles pin the pods to hosts with all of them', () => {
    expect(newAppYaml({ ...web, roles: ['Transcode'] }, false)).toContain('nodeSelector: { "homelab.io/transcode": "true" }')
    expect(newAppYaml({ ...web, roles: ['Storage', 'Transcode'] }, false)).toContain('nodeSelector: { "homelab.io/storage": "true", "homelab.io/transcode": "true" }')
    const host = (id: string, roles: Role[]) => ({ id, roles })
    const fleet = { a: host('ms01-01', ['ControlPlane', 'Storage', 'Compute', 'Vm']), b: host('minipc-01', ['Storage', 'Compute', 'Transcode']) }
    expect(hostsWithRoles(fleet, [])).toEqual(['minipc-01', 'ms01-01'])
    expect(hostsWithRoles(fleet, ['Storage', 'Transcode'])).toEqual(['minipc-01'])
    expect(hostsWithRoles(fleet, ['Vm', 'Transcode'])).toEqual([])
  })

  it('says what is wrong with the form', () => {
    expect(newAppProblems(BLANK_APP).map((p) => p.split(':')[0])).toEqual(['Name', 'Image'])
    const bad: NewApp = { ...web, name: 'Web_1', namespace: '-x', replicas: '0', port: '70000', ingressPath: 'app', volumeGi: '1.5', mountPath: 'data' }
    expect(newAppProblems(bad).map((p) => p.split(':')[0])).toEqual(['Name', 'Namespace', 'Replicas', 'Port', 'Web path', 'Volume', 'Mounted at'])
    expect(newAppProblems({ ...web, ingressPath: '/app' })).toEqual(['Web path: needs the port the container serves on'])
    expect(newAppProblems({ ...web, image: 'nginx 1.29' })).toHaveLength(1)
  })
})
