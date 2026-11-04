import { describe, expect, it } from 'vitest'
import { BLANK_VM, cloudInit, newVmProblems, newVmYaml } from './newvm'

const deb = { ...BLANK_VM, name: 'deb', namespace: 'lab', password: 's3cret "quoted"' }

describe('new VM', () => {
  it('says what keeps a VM from being written', () => {
    expect(newVmProblems(deb)).toEqual([])
    const bad = newVmProblems({ ...BLANK_VM, name: 'Deb', cores: '0', memoryGi: '0.5', user: 'Root', sshKey: 'a\nb', dataGi: 'x', port: '70000' })
    expect(bad.map((p) => p.split(':')[0])).toEqual(['Name', 'Cores', 'Memory', 'User', 'SSH key', 'Data disk', 'Port'])
    expect(newVmProblems({ ...deb, password: '' })).toEqual(['SSH key or password: one at least, or nobody can log in'])
  })

  it('makes the user with cloud-init, quoting what was typed, and installs the guest agent', () => {
    const ci = cloudInit({ ...deb, sshKey: 'ssh-ed25519 AAAA me' })
    expect(ci).toBe(
      '#cloud-config\nuser: "debian"\npassword: "s3cret \\"quoted\\""\nchpasswd: { expire: false }\nssh_authorized_keys:\n  - "ssh-ed25519 AAAA me"\nssh_pwauth: false\npackages: [qemu-guest-agent]\nruncmd:\n  - [systemctl, enable, --now, qemu-guest-agent]\n',
    )
    expect(cloudInit({ ...deb, password: '', sshKey: 'k' })).not.toContain('password')
  })

  it('writes a VM on the pod network booting its image', () => {
    const y = newVmYaml(deb, true)
    const docs = y.split('\n---\n')
    expect(docs.map((d) => /kind: (\w+)/.exec(d)?.[1])).toEqual(['Namespace', 'VirtualMachine'])
    const vm = docs[1]
    for (const line of [
      '  name: deb\n  namespace: lab\n',
      '  runStrategy: Always\n',
      '      labels: { app: deb }\n',
      '      evictionStrategy: LiveMigrateIfPossible\n',
      '        cpu: { cores: 1 }\n        memory: { guest: 1Gi }\n',
      '          disks:\n          - { name: root, disk: { bus: virtio } }\n          - { name: cloudinit, disk: { bus: virtio } }\n',
      '        - { name: root, containerDisk: { image: "quay.io/containerdisks/debian:13" } }\n',
    ]) {
      expect(vm).toContain(line)
    }
    expect(vm).not.toContain('nodeSelector')
    const userData = /userData: (".*")\n/.exec(vm)![1]
    expect(JSON.parse(userData)).toBe(cloudInit(deb))
  })

  it('a data disk is a Longhorn claim, on the storage network; a port is a Service', () => {
    const docs = newVmYaml({ ...deb, dataGi: '5', port: '8080' }, false).split('\n---\n')
    expect(docs.map((d) => /kind: (\w+)/.exec(d)?.[1])).toEqual(['PersistentVolumeClaim', 'VirtualMachine', 'Service'])
    expect(docs[0]).toContain('  name: deb-data\n  namespace: lab\n')
    expect(docs[0]).toContain('storageClassName: longhorn')
    expect(docs[0]).toContain('storage: 5Gi')
    expect(docs[1]).toContain('nodeSelector: { "homelab.io/storage-network": "true" }')
    expect(docs[1]).toContain('        - { name: data, persistentVolumeClaim: { claimName: deb-data } }\n')
    expect(docs[2]).toContain('  selector: { app: deb }\n  ports: [{ port: 8080, targetPort: 8080 }]')
  })
})
