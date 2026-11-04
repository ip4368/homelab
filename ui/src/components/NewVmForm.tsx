import { useState } from 'react'
import { BLANK_VM, newVmProblems, newVmYaml, type NewVm } from '../newvm'
import { useStore } from '../store'

/**
 * A VM from a containerDisk image, as a form. Like New from image, it only
 * writes the YAML: `onWrite` hands it to the Apply dialog to preview and apply.
 */
export function NewVmForm({ namespace, namespaces, onWrite }: { namespace: string; namespaces: string[]; onWrite(yaml: string, namespace: string): void }) {
  const [vm, setVm] = useState<NewVm>({ ...BLANK_VM, namespace })
  const kubevirt = useStore((s) => s.kube.kubevirt)
  const set = (patch: Partial<NewVm>) => setVm((v) => ({ ...v, ...patch }))
  const problems = newVmProblems(vm)
  const ns = vm.namespace || 'default'
  const isNew = ns !== 'default' && !namespaces.includes(ns)
  const field = (key: keyof NewVm, label: string, placeholder = '', size = 24, type = 'text') => (
    <input type={type} value={vm[key]} onChange={(e) => set({ [key]: e.target.value })} placeholder={placeholder} size={size} aria-label={label} />
  )

  return (
    <div>
      {kubevirt !== 'Deployed' && (
        <p className="warn">
          {kubevirt === null ? 'KubeVirt is not installed: homelabd installs it once a host has the vm role.' : `KubeVirt is ${kubevirt.toLowerCase()}: VMs start once it is deployed.`}
        </p>
      )}
      <div className="form-grid wide">
        <label>Name</label>
        <span>{field('name', 'vm name', 'debian')}</span>
        <label>Namespace</label>
        <span>
          <input value={vm.namespace} onChange={(e) => set({ namespace: e.target.value.trim() })} placeholder="default" list="apply-namespaces" size={24} aria-label="vm namespace" />
          {isNew && <span className="muted"> no pods there yet: the YAML creates it</span>}
        </span>
        <label>Image</label>
        <span>
          {field('image', 'vm image', '', 40)} <span className="muted">a containerDisk: the root disk, reset on every restart</span>
        </span>
        <label>Size</label>
        <span>
          {field('cores', 'cores', '1', 3)} cores, {field('memoryGi', 'memory GiB', '1', 3)} GiB memory
        </span>
        <label>User</label>
        <span>{field('user', 'vm user', 'debian', 12)}</span>
        <label>SSH key</label>
        <span>{field('sshKey', 'ssh key', 'ssh-ed25519 AAAA…', 48)}</span>
        <label>Password</label>
        <span>
          {field('password', 'vm password', 'none', 16, 'password')}{' '}
          <span className="muted">to log in on the serial console; it is stored in the VM's spec, which anyone with the kubeconfig can read</span>
        </span>
        <label>Data disk</label>
        <span>
          {field('dataGi', 'data disk GiB', 'none', 4)} GiB <span className="muted">a Longhorn volume that survives restarts; the VM then runs on hosts on the storage network</span>
        </span>
        <label>Port</label>
        <span>
          {field('port', 'vm port', 'none', 6)} <span className="muted">the guest serves on; gets a Service of the VM's name</span>
        </span>
      </div>
      <p className="muted">It runs on a host with the vm role, and moves to another while it runs if its disks allow, or else restarts there, when its host is drained.</p>
      {problems.length > 0 && vm.name !== '' && (
        <ul className="fixes warn">
          {problems.map((p) => (
            <li key={p}>{p}</li>
          ))}
        </ul>
      )}
      <div className="actions">
        <div className="spacer" />
        <button className="active" disabled={problems.length > 0} onClick={() => onWrite(newVmYaml(vm, isNew), ns)}>
          Write YAML
        </button>
      </div>
    </div>
  )
}
