import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: {
      '/ask': {
        target: 'http://107.22.140.160:8000',
        changeOrigin: true,
      },
      '/hello': {
        target: 'http://107.22.140.160:8000',
        changeOrigin: true,
      },
      '/history': {
        target: 'http://107.22.140.160:8000',
        changeOrigin: true,
      },
      '/session-docs': {
        target: 'http://107.22.140.160:8000',
        changeOrigin: true,
      },
      '/delete-session': {
        target: 'http://107.22.140.160:8000',
        changeOrigin: true,
      },
    }
  },
})
