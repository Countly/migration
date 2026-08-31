/** Synthetic drill_events doc generator — realistic shape/size mix (A/B test data). */

const APPS = Array.from({ length: 3 }, (_, i) => `5f${i}b2c3d4e5f6a7b8c9d0e1f${i}`);
const EVENTS = ['purchase_completed', 'level_up', 'item_viewed', 'search', 'cart_add', 'video_played', 'settings_changed', 'share'];
const PLATFORMS = ['Android', 'iOS', 'Windows', 'Macintosh'];
const COUNTRIES = ['US', 'DE', 'TR', 'GB', 'FR', 'JP', 'BR', 'IN'];
const VIEWS = ['/home', '/product/detail', '/checkout', '/profile', '/search/results', '/settings'];

let seed = 0x9e3779b9;
function rnd(): number {
  // mulberry32 — full 32-bit period, no float precision loss
  seed = (seed + 0x6d2b79f5) | 0;
  let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
  t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
}
function pick<T>(arr: T[]): T { return arr[Math.floor(rnd() * arr.length)]; }
function hex(n: number): string {
  let s = '';
  for (let i = 0; i < n; i++) s += Math.floor(rnd() * 16).toString(16);
  return s;
}

export interface GenOptions { days?: number; startTs?: number }

export function generateDoc(i: number, opts: GenOptions = {}): Record<string, unknown> {
  const days = opts.days ?? 30;
  const start = opts.startTs ?? Date.UTC(2026, 5, 1);
  const ts = start + Math.floor(rnd() * days * 86400_000);
  const uid = String(1 + Math.floor(rnd() * 50000));
  const did = `${hex(8)}-${hex(4)}-${hex(4)}-${hex(4)}-${hex(12)}`;
  const r = rnd();
  const a = pick(APPS);

  let e: string; let sg: Record<string, unknown>;
  if (r < 0.6) {
    e = pick(EVENTS);
    sg = {
      category: pick(['electronics', 'clothing', 'books', 'toys', 'garden']),
      price: Math.round(rnd() * 20000) / 100,
      currency: pick(['USD', 'EUR', 'TRY']),
      quantity: 1 + Math.floor(rnd() * 5),
      source: pick(['organic', 'push', 'deeplink', 'widget']),
      logged_in: rnd() > 0.5,
      ab_variant: pick(['control', 'variant_a', 'variant_b']),
      item_id: `SKU-${Math.floor(rnd() * 100000)}`,
    };
  } else if (r < 0.75) {
    e = '[CLY]_view';
    sg = { name: pick(VIEWS), visit: 1, start: rnd() > 0.8 ? 1 : 0, bounce: rnd() > 0.9 ? 1 : 0, segment: pick(PLATFORMS) };
  } else if (r < 0.85) {
    e = '[CLY]_session';
    sg = { session_id: hex(16) };
  } else if (r < 0.93) {
    e = '[CLY]_action';
    sg = { type: 'click', x: Math.floor(rnd() * 1080), y: Math.floor(rnd() * 1920), width: 1080, height: 1920, view: pick(VIEWS) };
  } else if (r < 0.97) {
    e = '[CLY]_crash';
    sg = { group: hex(32), fatal: rnd() > 0.5, os: pick(PLATFORMS), app_version: `4.${Math.floor(rnd() * 10)}.0` };
  } else {
    e = '[CLY]_nps';
    sg = { widget_id: hex(24), rating: Math.floor(rnd() * 11), platform: pick(PLATFORMS) };
  }

  const doc: Record<string, unknown> = {
    _id: `${hex(40)}_${uid}_${ts}_${i}`,
    a, e, uid, did,
    ts,
    cd: new Date(ts + Math.floor(rnd() * 30000)),
    lu: new Date(ts + Math.floor(rnd() * 60000)),
    up: {
      p: pick(PLATFORMS),
      pv: `p${Math.floor(rnd() * 15)}.${Math.floor(rnd() * 9)}`,
      d: pick(['iPhone14,2', 'SM-G991B', 'Pixel 7', 'iPad13,4']),
      av: `4.${Math.floor(rnd() * 12)}.${Math.floor(rnd() * 9)}`,
      cc: pick(COUNTRIES),
      cty: pick(['Istanbul', 'Berlin', 'London', 'Tokyo', 'Austin', 'Unknown']),
      la: pick(['en', 'de', 'tr', 'ja', 'pt']),
      src: pick(['com.android.vending', 'App Store', 'web']),
      dnst: pick(['wifi', '4g', '5g']),
      brw: pick(['Chrome', 'Safari', 'Firefox']),
      fs: ts - Math.floor(rnd() * 300 * 86400_000),
      ls: ts - Math.floor(rnd() * 30 * 86400_000),
      sc: Math.floor(rnd() * 500),
    },
    c: 1,
  };
  if (rnd() > 0.7) doc.s = Math.round(rnd() * 10000) / 100;
  if (rnd() > 0.6) doc.dur = Math.round(rnd() * 600000) / 1000;
  if (rnd() > 0.8) doc.custom = { plan: pick(['free', 'pro', 'enterprise']), seats: 1 + Math.floor(rnd() * 50) };
  if (rnd() > 0.9) doc.cmp = { c: pick(['summer_sale', 'onboarding_v2']), m: pick(['email', 'push']) };
  return doc;
}
