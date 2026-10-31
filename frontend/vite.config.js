import process from 'node:process'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
plugins: [react()],
  resolve: {
    dedupe: ['react', 'react-dom'], 
  },
  optimizeDeps: {
    include: ['react-grid-layout', 'roslib']
  },
  server: {
    // W produkcji /api/* i /video/* obsluguje reverse proxy na tym samym origin.
    // W dev Vite udaje to proxy; cele mozna nadpisac zmiennymi srodowiskowymi.
    proxy: {
      '/api': {
        target: process.env.DEV_API_TARGET || 'http://localhost:3000',
        changeOrigin: true,
      },
      '/video': {
        target: process.env.DEV_VIDEO_TARGET || 'http://localhost:8889',
        changeOrigin: true,
        secure: false,
      },
    },
  },
})
