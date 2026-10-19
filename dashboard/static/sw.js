// Minimal service worker: it lets phones install TriageIQ like an app.
// Everything still comes live from the server (a support console must never show stale tickets).
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', e => e.waitUntil(self.clients.claim()));
self.addEventListener('fetch', () => {});