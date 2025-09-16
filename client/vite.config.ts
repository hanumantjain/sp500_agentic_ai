import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: {
      '/ask': {
        target: 'https://bluejai-ai-server.vercel.app',
        changeOrigin: true,
      },
      '/hello': {
        target: 'https://bluejai-ai-server.vercel.app',
        changeOrigin: true,
      },
      '/history': {
        target: 'https://bluejai-ai-server.vercel.app',
        changeOrigin: true,
      },
      '/session-docs': {
        target: 'https://bluejai-ai-server.vercel.app',
        changeOrigin: true,
      },
      '/delete-session': {
        target: 'https://bluejai-ai-server.vercel.app',
        changeOrigin: true,
      },
    },
  },
})
