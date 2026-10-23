import react from '@vitejs/plugin-react'
import { defineConfig } from 'vitest/config'

// The dev server forwards the API (including the WebSocket) to the Rust server.
const target = `http://localhost:${process.env.PORT || 8080}`

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: { '/api': { target, ws: true } },
  },
  test: { environment: 'node' },
})
