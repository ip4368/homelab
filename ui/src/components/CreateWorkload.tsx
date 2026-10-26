import { useMemo, useState } from 'react'
import { op } from '../api'
import { ALL_ROLES } from '../eligibility'
import type { ApiOp } from '../gen/ApiOp'
import type { Role } from '../gen/Role'
import { useStore } from '../store'
import { MAX_DEPLOYMENT_REPLICAS, MAX_VOLUME_REPLICAS, validateDeployment, validateVm, validateVolume } from '../workloads'
import { useAction } from './useAction'

type Tab = 'VM' | 'Deployment' | 'Volume'
const TABS: Tab[] = ['VM', 'Deployment', 'Volume']

/** A number box that keeps whole numbers, so 0 and out-of-range values reach the validator as they are typed. */
function NumberField({ label, value, onChange, min, max }: { label: string; value: number; onChange(n: number): void; min?: number; max?: number }) {
  return (
    <>
      <label>{label}</label>
      <input
        type="number"
        min={min}
        max={max}
        value={value}
        onChange={(e) => onChange(Math.floor(Number(e.target.value)) || 0)}
        aria-label={label}
        style={{ width: 90 }}
      />
    </>
  )
}

/** The submit row shared by the forms: the button, the reason it is disabled, the server's refusal. */
function Submit({ label, problem, build }: { label: string; problem: string | null; build(): ApiOp }) {
  const { error, busy, run } = useAction()
  const [created, setCreated] = useState<string | null>(null)
  const submit = () =>
    run(async () => {
      setCreated(null)
      await op(build())
      setCreated(label)
    })
  return (
    <div className="actions">
      <button className="active" disabled={busy || problem !== null} onClick={submit}>
        Create
      </button>
      {problem && <span className="muted">{problem}</span>}
      {error && (
        <span className="bad" role="alert">
          {error}
        </span>
      )}
      {created && !error && <span className="ok">Created {created}.</span>}
    </div>
  )
}

function VolumeForm() {
  const volumes = useStore((s) => s.view.volumes)
  const [id, setId] = useState('')
  const [sizeGb, setSizeGb] = useState(20)
  const [replicas, setReplicas] = useState(3)
  const volume = { id: id.trim(), size_gb: sizeGb, replicas }
  return (
    <>
      <div className="form-grid wide">
        <label>Name</label>
        <input value={id} onChange={(e) => setId(e.target.value)} aria-label="volume name" size={16} />
        <NumberField label="Size (GB)" value={sizeGb} onChange={setSizeGb} min={1} />
        <NumberField label="Replicas" value={replicas} onChange={setReplicas} min={1} max={MAX_VOLUME_REPLICAS} />
      </div>
      <Submit label={volume.id} problem={validateVolume(volume, Object.keys(volumes))} build={() => ({ CreateVolume: { volume } })} />
    </>
  )
}

function DeploymentForm() {
  const deployments = useStore((s) => s.view.deployments)
  const [id, setId] = useState('')
  const [image, setImage] = useState('nginx:1.27')
  const [replicas, setReplicas] = useState(3)
  const [cpu, setCpu] = useState(250)
  const [ram, setRam] = useState(256)
  const [roles, setRoles] = useState<Role[]>([])
  const [prefix, setPrefix] = useState('')
  const deployment = {
    id: id.trim(),
    image: image.trim(),
    replicas,
    cpu_m: cpu,
    ram_mb: ram,
    selector: { roles, capability_prefix: prefix.trim() === '' ? null : prefix.trim() },
  }
  // A pod always needs Compute, so it is not a choice.
  const extraRoles = ALL_ROLES.filter((r) => r !== 'Compute')
  return (
    <>
      <div className="form-grid wide">
        <label>Name</label>
        <input value={id} onChange={(e) => setId(e.target.value)} aria-label="deployment name" size={16} />
        <label>Image</label>
        <input value={image} onChange={(e) => setImage(e.target.value)} aria-label="image" size={24} />
        <NumberField label="Replicas" value={replicas} onChange={setReplicas} min={1} max={MAX_DEPLOYMENT_REPLICAS} />
        <NumberField label="CPU (millicores)" value={cpu} onChange={setCpu} min={1} />
        <NumberField label="RAM (MB)" value={ram} onChange={setRam} min={1} />
        <label>Also needs roles</label>
        <span className="role-picker">
          {extraRoles.map((r) => (
            <label key={r} className="inline">
              <input
                type="checkbox"
                checked={roles.includes(r)}
                onChange={(e) => setRoles(ALL_ROLES.filter((x) => (x === r ? e.target.checked : roles.includes(x))))}
              />
              {r}
            </label>
          ))}
        </span>
        <label>Capability prefix</label>
        <input value={prefix} onChange={(e) => setPrefix(e.target.value)} placeholder="hw-transcode:" aria-label="capability prefix" size={16} />
      </div>
      <Submit label={deployment.id} problem={validateDeployment(deployment, Object.keys(deployments))} build={() => ({ CreateDeployment: { deployment } })} />
    </>
  )
}

function VmForm() {
  const vms = useStore((s) => s.view.vms)
  const volumes = useStore((s) => s.view.volumes)
  const networks = useStore((s) => s.view.networks)
  const [id, setId] = useState('')
  const [vcpu, setVcpu] = useState(2)
  const [ram, setRam] = useState(4096)
  const [disk, setDisk] = useState(20)
  const [volume, setVolume] = useState('')
  const [chosen, setChosen] = useState<string[] | null>(null)
  const volumeIds = useMemo(() => Object.keys(volumes).sort(), [volumes])
  const vmNetworks = useMemo(() => Object.keys(networks).filter((n) => n.startsWith('vm-')).sort(), [networks])
  // Until the user chooses, the VM joins vm-lan (or the first VM network) and the first volume.
  const nets = chosen ?? (vmNetworks.includes('vm-lan') ? ['vm-lan'] : vmNetworks.slice(0, 1))
  const vol = volumeIds.includes(volume) ? volume : (volumeIds[0] ?? '')
  const vm = { id: id.trim(), vcpu, ram_mb: ram, disk_gb: disk, networks: nets, volume: vol }
  return (
    <>
      <div className="form-grid wide">
        <label>Name</label>
        <input value={id} onChange={(e) => setId(e.target.value)} aria-label="vm name" size={16} />
        <NumberField label="vCPU" value={vcpu} onChange={setVcpu} min={1} />
        <NumberField label="RAM (MB)" value={ram} onChange={setRam} min={1} />
        <NumberField label="Disk (GB)" value={disk} onChange={setDisk} min={1} />
        <label>Disk volume</label>
        <select value={vol} onChange={(e) => setVolume(e.target.value)} aria-label="disk volume">
          {volumeIds.length === 0 && <option value="">(create a volume first)</option>}
          {volumeIds.map((v) => (
            <option key={v}>{v}</option>
          ))}
        </select>
        <label>Networks</label>
        <span className="role-picker">
          {vmNetworks.map((n) => (
            <label key={n} className="inline">
              <input
                type="checkbox"
                checked={nets.includes(n)}
                onChange={(e) => setChosen(vmNetworks.filter((x) => (x === n ? e.target.checked : nets.includes(x))))}
              />
              {n}
            </label>
          ))}
        </span>
      </div>
      <Submit label={vm.id} problem={validateVm(vm, Object.keys(vms), volumeIds)} build={() => ({ CreateVm: { vm } })} />
    </>
  )
}

export function CreateWorkload() {
  const [tab, setTab] = useState<Tab>('VM')
  return (
    <div className="panel create-workload">
      <div className="tabs inline-tabs" role="tablist">
        {TABS.map((t) => (
          <button key={t} role="tab" aria-selected={t === tab} className={t === tab ? 'active' : ''} onClick={() => setTab(t)}>
            {t}
          </button>
        ))}
      </div>
      {tab === 'VM' && <VmForm />}
      {tab === 'Deployment' && <DeploymentForm />}
      {tab === 'Volume' && <VolumeForm />}
    </div>
  )
}
