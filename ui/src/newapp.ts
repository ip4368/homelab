import type { HostObj } from './gen/HostObj'
import type { Role } from './gen/Role'

/** The New from image form, as typed. */
export interface NewApp {
  name: string
  /** Empty for `default`. */
  namespace: string
  image: string
  replicas: string
  /** The container's port; empty for none (no Service, no Ingress). */
  port: string
  /** Serve it at `http://<vip><path>` through K3s's Traefik; empty for no Ingress. */
  ingressPath: string
  /** A Longhorn volume per pod, in GiB; empty for none (a Deployment, not a StatefulSet). */
  volumeGi: string
  mountPath: string
  /** Only on hosts with all of these roles; none for any host. */
  roles: Role[]
}

export const BLANK_APP: NewApp = { name: '', namespace: '', image: '', replicas: '1', port: '', ingressPath: '', volumeGi: '', mountPath: '/data', roles: [] }

/** The node label homelabd sets on hosts with each role (`homelab.io/<role>=true`). */
export const ROLE_LABELS: Record<Role, string> = {
  ControlPlane: 'homelab.io/control-plane',
  Storage: 'homelab.io/storage',
  Compute: 'homelab.io/compute',
  Vm: 'homelab.io/vm',
  Transcode: 'homelab.io/transcode',
}

/** A DNS-1035 label: what a Service name (and so the app's name) must be. */
const LABEL = /^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$/

const int = (s: string) => (/^\d+$/.test(s.trim()) ? Number(s) : NaN)

/** What keeps `a` from making a manifest, in words; none if it can. */
export function newAppProblems(a: NewApp): string[] {
  const out: string[] = []
  if (!LABEL.test(a.name)) out.push('Name: lowercase letters, digits and -, starting with a letter, at most 63')
  if (a.namespace !== '' && !LABEL.test(a.namespace)) out.push('Namespace: lowercase letters, digits and -, starting with a letter, at most 63')
  if (a.image.trim() === '' || /\s/.test(a.image.trim())) out.push('Image: a container image, such as nginx:1.29')
  const replicas = int(a.replicas)
  if (!(replicas >= 1)) out.push('Replicas: 1 or more')
  const port = a.port.trim() === '' ? null : int(a.port)
  if (port !== null && !(port >= 1 && port <= 65535)) out.push('Port: 1 to 65535')
  if (a.ingressPath.trim() !== '') {
    if (!a.ingressPath.startsWith('/') || /\s/.test(a.ingressPath)) out.push('Web path: starts with /, such as /app')
    if (port === null) out.push('Web path: needs the port the container serves on')
  }
  if (a.volumeGi.trim() !== '') {
    if (!(int(a.volumeGi) >= 1)) out.push('Volume: a whole number of GiB, 1 or more')
    if (!a.mountPath.startsWith('/')) out.push('Mounted at: an absolute path, such as /data')
  }
  return out
}

/**
 * The manifests for `a` (which must have no problems): a Deployment, or a
 * StatefulSet with a Longhorn volume per pod on the storage network's hosts,
 * in both cases only on hosts with all of `a.roles`; a Service if it has a port, and an Ingress if it has a web path. Pods leave
 * a dead host after 20 s. With `newNamespace`, the Namespace too.
 */
export function newAppYaml(a: NewApp, newNamespace: boolean): string {
  const q = JSON.stringify // a YAML double-quoted string
  const name = a.name
  const ns = a.namespace || 'default'
  const port = a.port.trim() === '' ? null : int(a.port)
  const volume = a.volumeGi.trim() === '' ? null : int(a.volumeGi)
  const meta = [`metadata:`, `  name: ${name}`, `  namespace: ${ns}`]
  const docs: string[][] = []
  if (newNamespace) docs.push([`apiVersion: v1`, `kind: Namespace`, `metadata:`, `  name: ${ns}`])

  const selector = [...(volume !== null ? ['homelab.io/storage-network'] : []), ...a.roles.map((r) => ROLE_LABELS[r])].map((l) => `${q(l)}: "true"`)
  docs.push([
    `apiVersion: apps/v1`,
    `kind: ${volume !== null ? 'StatefulSet' : 'Deployment'}`,
    ...meta,
    `spec:`,
    ...(volume !== null ? [`  serviceName: ${name}`] : []),
    `  replicas: ${int(a.replicas)}`,
    `  selector:`,
    `    matchLabels: { app: ${name} }`,
    `  template:`,
    `    metadata:`,
    `      labels: { app: ${name} }`,
    `    spec:`,
    ...(selector.length ? [`      nodeSelector: { ${selector.join(', ')} }`] : []),
    `      tolerations:`,
    `        - { key: node.kubernetes.io/unreachable, operator: Exists, effect: NoExecute, tolerationSeconds: 20 }`,
    `        - { key: node.kubernetes.io/not-ready, operator: Exists, effect: NoExecute, tolerationSeconds: 20 }`,
    `      containers:`,
    `        - name: ${name}`,
    `          image: ${q(a.image.trim())}`,
    ...(port !== null ? [`          ports: [{ containerPort: ${port} }]`] : []),
    ...(volume !== null ? [`          volumeMounts: [{ name: data, mountPath: ${q(a.mountPath)} }]`] : []),
    ...(volume !== null
      ? [
          `  volumeClaimTemplates:`,
          `    - metadata: { name: data }`,
          `      spec:`,
          `        accessModes: [ReadWriteOnce]`,
          `        storageClassName: longhorn`,
          `        resources: { requests: { storage: ${volume}Gi } }`,
        ]
      : []),
  ])
  if (port !== null) {
    docs.push([`apiVersion: v1`, `kind: Service`, ...meta, `spec:`, `  selector: { app: ${name} }`, `  ports: [{ port: ${port} }]`])
  }
  if (port !== null && a.ingressPath.trim() !== '') {
    docs.push([
      `apiVersion: networking.k8s.io/v1`,
      `kind: Ingress`,
      ...meta,
      `spec:`,
      `  rules:`,
      `    - http:`,
      `        paths:`,
      `          - path: ${q(a.ingressPath)}`,
      `            pathType: Prefix`,
      `            backend: { service: { name: ${name}, port: { number: ${port} } } }`,
    ])
  }
  return docs.map((d) => d.join('\n') + '\n').join('---\n')
}

/** The hosts that have every one of `roles` (all of them for none), by id. */
export function hostsWithRoles(hosts: Record<string, Pick<HostObj, 'id' | 'roles'>>, roles: Role[]): string[] {
  return Object.values(hosts)
    .filter((h) => roles.every((r) => h.roles.includes(r)))
    .map((h) => h.id)
    .sort()
}
