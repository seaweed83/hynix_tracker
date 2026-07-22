const CACHE = 'sk-xn-v1'
const ASSETS = [
  '/',
  '/intraday',
  '/xn',
  '/report/v3',
  '/static/manifest.json',
  '/static/icon-192.png',
  '/static/icon-512.png'
]

self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE).then(c => c.addAll(ASSETS)).then(() => self.skipWaiting())
  )
})

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys => Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k))))
  )
})

self.addEventListener('fetch', e => {
  const url = new URL(e.request.url)
  if (url.pathname.startsWith('/api/')) {
    e.respondWith(fetch(e.request).catch(() => new Response(JSON.stringify({error:'offline'}), {status:503,headers:{'Content-Type':'application/json'}})))
    return
  }
  e.respondWith(
    caches.match(e.request).then(r => r || fetch(e.request))
  )
})
