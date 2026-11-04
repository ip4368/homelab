/** The New VM form, as typed. */
export interface NewVm {
  name: string
  /** Empty for `default`. */
  namespace: string
  /** A containerDisk image: the VM's root disk, reset on every restart. */
  image: string
  cores: string
  memoryGi: string
  /** The guest's user, made by cloud-init. */
  user: string
  /** Its SSH public key; empty for none. */
  sshKey: string
  /** Its password, for the serial console; empty for none. */
  password: string
  /** A Longhorn data disk, in GiB, that survives restarts; empty for none. */
  dataGi: string
  /** A port the guest serves on, exposed by a Service of the VM's name; empty for none. */
  port: string
}

export const DEFAULT_VM_IMAGE = 'quay.io/containerdisks/debian:13'

export const BLANK_VM: NewVm = { name: '', namespace: '', image: DEFAULT_VM_IMAGE, cores: '1', memoryGi: '1', user: 'debian', sshKey: '', password: '', dataGi: '', port: '' }

/** A DNS-1035 label: what a Service name (and so the VM's name) must be. */
const LABEL = /^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$/
/** A Linux user name, as Debian's adduser accepts it. */
const USER = /^[a-z_][a-z0-9_-]{0,31}$/

const int = (s: string) => (/^\d+$/.test(s.trim()) ? Number(s) : NaN)

/** What keeps `v` from making a manifest, in words; none if it can. */
export function newVmProblems(v: NewVm): string[] {
  const out: string[] = []
  if (!LABEL.test(v.name)) out.push('Name: lowercase letters, digits and -, starting with a letter, at most 63')
  if (v.namespace !== '' && !LABEL.test(v.namespace)) out.push('Namespace: lowercase letters, digits and -, starting with a letter, at most 63')
  if (v.image.trim() === '' || /\s/.test(v.image.trim())) out.push('Image: a containerDisk image, such as ' + DEFAULT_VM_IMAGE)
  if (!(int(v.cores) >= 1)) out.push('Cores: 1 or more')
  if (!(int(v.memoryGi) >= 1)) out.push('Memory: a whole number of GiB, 1 or more')
  if (!USER.test(v.user)) out.push('User: lowercase letters, digits, _ and -, at most 32')
  if (v.sshKey.trim() === '' && v.password === '') out.push('SSH key or password: one at least, or nobody can log in')
  if (v.sshKey.includes('\n')) out.push('SSH key: one line')
  if (v.dataGi.trim() !== '' && !(int(v.dataGi) >= 1)) out.push('Data disk: a whole number of GiB, 1 or more')
  const port = v.port.trim() === '' ? null : int(v.port)
  if (port !== null && !(port >= 1 && port <= 65535)) out.push('Port: 1 to 65535')
  return out
}

/** cloud-init's user data for `v`: its user, key and password, and the guest agent KubeVirt reads the guest through. */
export function cloudInit(v: NewVm): string {
  const q = JSON.stringify
  const lines = ['#cloud-config', `user: ${q(v.user)}`]
  if (v.password !== '') lines.push(`password: ${q(v.password)}`, 'chpasswd: { expire: false }')
  if (v.sshKey.trim() !== '') lines.push('ssh_authorized_keys:', `  - ${q(v.sshKey.trim())}`)
  lines.push('ssh_pwauth: false', 'packages: [qemu-guest-agent]', 'runcmd:', '  - [systemctl, enable, --now, qemu-guest-agent]')
  return lines.join('\n') + '\n'
}

/**
 * The manifests for `v` (which must have no problems): a VirtualMachine that
 * runs always, on the pod network, booting `v.image` with cloud-init; a
 * Longhorn claim for a data disk (then only on hosts on the storage
 * network, where Longhorn runs); a Service if it has a port. With
 * `newNamespace`, the Namespace too.
 */
export function newVmYaml(v: NewVm, newNamespace: boolean): string {
  const q = JSON.stringify // a YAML double-quoted string
  const name = v.name
  const ns = v.namespace || 'default'
  const data = v.dataGi.trim() === '' ? null : int(v.dataGi)
  const port = v.port.trim() === '' ? null : int(v.port)
  const meta = [`metadata:`, `  name: ${name}`, `  namespace: ${ns}`]
  const docs: string[][] = []
  if (newNamespace) docs.push([`apiVersion: v1`, `kind: Namespace`, `metadata:`, `  name: ${ns}`])
  if (data !== null) {
    docs.push([
      `apiVersion: v1`,
      `kind: PersistentVolumeClaim`,
      `metadata:`,
      `  name: ${name}-data`,
      `  namespace: ${ns}`,
      `spec:`,
      `  accessModes: [ReadWriteOnce]`,
      `  storageClassName: longhorn`,
      `  resources: { requests: { storage: ${data}Gi } }`,
    ])
  }
  const disk = (n: string) => `          - { name: ${n}, disk: { bus: virtio } }`
  docs.push([
    `apiVersion: kubevirt.io/v1`,
    `kind: VirtualMachine`,
    ...meta,
    `spec:`,
    `  runStrategy: Always`,
    `  template:`,
    `    metadata:`,
    `      labels: { app: ${name} }`,
    `    spec:`,
    `      evictionStrategy: LiveMigrateIfPossible`,
    ...(data !== null ? [`      nodeSelector: { "homelab.io/storage-network": "true" }`] : []),
    `      domain:`,
    `        cpu: { cores: ${int(v.cores)} }`,
    `        memory: { guest: ${int(v.memoryGi)}Gi }`,
    `        devices:`,
    `          disks:`,
    disk('root'),
    ...(data !== null ? [disk('data')] : []),
    disk('cloudinit'),
    `          interfaces: [{ name: default, masquerade: {} }]`,
    `      networks: [{ name: default, pod: {} }]`,
    `      volumes:`,
    `        - { name: root, containerDisk: { image: ${q(v.image.trim())} } }`,
    ...(data !== null ? [`        - { name: data, persistentVolumeClaim: { claimName: ${name}-data } }`] : []),
    `        - name: cloudinit`,
    `          cloudInitNoCloud:`,
    `            userData: ${q(cloudInit(v))}`,
  ])
  if (port !== null) {
    docs.push([`apiVersion: v1`, `kind: Service`, ...meta, `spec:`, `  selector: { app: ${name} }`, `  ports: [{ port: ${port}, targetPort: ${port} }]`])
  }
  return docs.map((d) => d.join('\n')).join('\n---\n') + '\n'
}
