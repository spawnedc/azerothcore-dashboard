import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: { host: true, // todo: make these read from config?
            port: 5173 },
  preview: { host: true, port: 80, allowedHosts: true },
});
