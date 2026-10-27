// A minimal Kubernetes API client for the e2e run: the admin kubeconfig's
// server, CA and client certificate, over node:https. No dependencies.

import https from 'node:https'

export interface KubeResponse {
  status: number
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  body: any
  text: string
}

/** One `key: value` line of a kubeconfig (the fields we need are scalars). */
function field(yaml: string, key: string): string {
  const m = new RegExp(`^\\s*${key}:\\s*(\\S+)\\s*$`, 'm').exec(yaml)
  if (!m) throw new Error(`kubeconfig has no ${key}`)
  return m[1]
}

export class Kube {
  readonly server: URL
  private agent: https.Agent

  constructor(kubeconfig: string) {
    this.server = new URL(field(kubeconfig, 'server'))
    const b64 = (k: string) => Buffer.from(field(kubeconfig, k), 'base64')
    this.agent = new https.Agent({
      ca: b64('certificate-authority-data'),
      cert: b64('client-certificate-data'),
      key: b64('client-key-data'),
      keepAlive: false,
    })
  }

  /** Sends one request; a network error or timeout comes back as status 0. */
  req(method: string, path: string, body?: unknown, contentType = 'application/json', timeoutMs = 15_000): Promise<KubeResponse> {
    return new Promise((resolve) => {
      const data = body === undefined ? undefined : typeof body === 'string' ? body : JSON.stringify(body)
      const r = https.request(
        {
          host: this.server.hostname,
          port: this.server.port || 443,
          path,
          method,
          agent: this.agent,
          timeout: timeoutMs,
          headers: { accept: 'application/json', ...(data ? { 'content-type': contentType, 'content-length': Buffer.byteLength(data) } : {}) },
        },
        (res) => {
          let text = ''
          res.setEncoding('utf8')
          res.on('data', (c) => (text += c))
          res.on('end', () => {
            let parsed: unknown = undefined
            try {
              parsed = JSON.parse(text)
            } catch {
              // not JSON (logs, readyz)
            }
            resolve({ status: res.statusCode ?? 0, body: parsed, text })
          })
        },
      )
      r.on('timeout', () => r.destroy(new Error('timeout')))
      r.on('error', (e) => resolve({ status: 0, body: undefined, text: e.message }))
      if (data) r.write(data)
      r.end()
    })
  }

  async get(path: string) {
    return this.req('GET', path)
  }

  /** GET that must succeed; its JSON. */
  async must(path: string) {
    const r = await this.get(path)
    if (r.status !== 200) throw new Error(`GET ${path}: ${r.status} ${r.text.slice(0, 200)}`)
    return r.body
  }

  /** Creates `obj` under `path`; an existing one is fine. */
  async create(path: string, obj: unknown) {
    const r = await this.req('POST', path, obj)
    if (r.status !== 201 && r.status !== 200 && r.status !== 409) throw new Error(`POST ${path}: ${r.status} ${r.text.slice(0, 300)}`)
    return r.body
  }

  async delete(path: string) {
    return this.req('DELETE', path)
  }
}
