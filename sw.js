/* Fantasy Scores — service worker
 * Adapted from the Over the Ivy PWA. Strategies:
 *   - Network-first for HTML documents (a stale shell breaks updates),
 *     falling back to cache only when offline.
 *   - Cache-first for same-origin static assets (icons, manifest) — safe
 *     because they change rarely and we bump CACHE on release.
 *   - Never intercept Sleeper — api.sleeper.app / sleepercdn.com are live
 *     data and go straight to the network.
 * Bump CACHE when this file changes to force the new worker to install.
 */
const CACHE = "ffscores-v1";
const ASSETS = [
  "./", "./index.html", "./manifest.json",
  "./icon-192.png", "./icon-512.png", "./apple-touch-icon.png"
];

self.addEventListener("install", (event) => {
  event.waitUntil(caches.open(CACHE).then((c) => c.addAll(ASSETS)).catch(() => {}));
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

function isHtml(req) {
  if (req.mode === "navigate") return true;
  const accept = req.headers.get("accept") || "";
  return accept.includes("text/html");
}

self.addEventListener("fetch", (event) => {
  const { request } = event;
  if (request.method !== "GET") return;

  let url;
  try { url = new URL(request.url); } catch (e) { return; }

  // Live fantasy data — always straight to the network, never cached.
  if (url.hostname.endsWith("sleeper.app") || url.hostname.endsWith("sleepercdn.com")) return;

  // Only manage our own origin.
  if (url.origin !== self.location.origin) return;

  // HTML: network-first, cache fallback when offline.
  if (isHtml(request)) {
    event.respondWith(
      fetch(request)
        .then((res) => {
          const copy = res.clone();
          caches.open(CACHE).then((c) => c.put(request, copy)).catch(() => {});
          return res;
        })
        .catch(() => caches.match(request).then((m) => m || caches.match("./index.html")))
    );
    return;
  }

  // Static assets: cache-first, then fill.
  event.respondWith(
    caches.match(request).then((hit) => {
      if (hit) return hit;
      return fetch(request).then((res) => {
        const copy = res.clone();
        caches.open(CACHE).then((c) => c.put(request, copy)).catch(() => {});
        return res;
      }).catch(() => undefined);
    })
  );
});
