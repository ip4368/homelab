import { useStore } from '../store'
import { KubeWorkloads } from './KubeWorkloads'

/** What Kubernetes runs, as the server reads it from the K3s API. There is no other workload plane. */
export function WorkloadsView({ onSelectHost }: { onSelectHost(id: string): void }) {
  // Only a server with a lab reads Kubernetes; without one there is nothing to show.
  const hasKube = useStore((s) => s.kube.at > 0 || s.kube.error !== null)
  if (!hasKube) return <p className="muted">Kubernetes has not been read yet.</p>
  return <KubeWorkloads onSelectHost={onSelectHost} />
}
