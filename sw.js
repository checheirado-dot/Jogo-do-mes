// HaxCareer: busca a versão nova na internet primeiro; usa a cópia guardada só se estiver sem internet.
const CACHE = "hax-v2";

self.addEventListener("install", () => self.skipWaiting());

self.addEventListener("activate", e => {
  e.waitUntil((async () => {
    for (const k of await caches.keys()) if (k !== CACHE) await caches.delete(k);
    await self.clients.claim();
  })());
});

self.addEventListener("fetch", e => {
  const r = e.request;
  if (r.method !== "GET" || new URL(r.url).origin !== location.origin) return;
  e.respondWith((async () => {
    try {
      const res = await fetch(r, { cache: "no-store" }); // ignora o cache do navegador
      if (res.ok) { const c = await caches.open(CACHE); c.put(r, res.clone()); }
      return res;
    } catch (err) {
      return (await caches.match(r, { ignoreSearch: true })) || Response.error();
    }
  })());
});
