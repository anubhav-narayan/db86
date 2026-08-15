import { defineConfig } from 'vite';

export default defineConfig({
  base: './',
  server: {
    port: 5173,
    proxy: {
      '/databases': {
        target: 'http://127.0.0.1:8000',
        changeOrigin: true,
      },
      '/api/health': {
        target: 'http://127.0.0.1:8000',
        changeOrigin: true,
        rewrite: (path) => '/',
      },
    },
  },
});
