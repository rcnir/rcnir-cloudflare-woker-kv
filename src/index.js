addEventListener('fetch', e => e.respondWith(handle(e.request)));
async function handle(request, env) {
  const ua = request.headers.get('User-Agent') || 'UA_NOT_FOUND';
  const ip = request.headers.get('CF-Connecting-IP') || 'IP_NOT_FOUND';
  const { pathname, search } = new URL(request.url);
  const path = pathname.toLowerCase();

  // 1. ロケール抽出（例: /en-us/... → en-us、なければ "root"）
  const locale = extractLocale(path);

  // 2. 画像 / 静的 / Shopify 内部系はスキップ
  const EXT_SKIP = /\.(jpg|jpeg|png|gif|svg|webp|js|css|woff2?|ttf|ico|map|txt|eot|otf|json|xml|avif)(\?|$)/;
  const PATH_SKIP = path.startsWith('/wpm@') ||
                    path.includes('/cart.js') ||
                    path.includes('/recommendations/') ||
                    path.startsWith('/_t/');
  if (EXT_SKIP.test(path) || PATH_SKIP) return fetch(request);

  // 3. ロケールファンアウトチェック（多言語爆撃対策）
  //    30秒以内に 3 ロケール以上アクセスで 24h ブロック
  const fanout = await localeFanoutCheck(ip, locale, env);
  if (!fanout.allow) {
    console.log(`[LF-BLOCK] ${request.url} IP=${ip} UA=${ua} reason=${fanout.reason||'locale-fanout'}`);
    // 静かに 404 / 429 など
    return new Response('Not Found', { status: 404 });
  }

  // 4. Bot ラベル付与
  const botPattern = /(bot|crawl|spider|slurp|fetch|headless|preview|externalagent|barkrowler|bingbot|petalbot|python-requests|aiohttp|monitor|insights)/i;
  const isBot = botPattern.test(ua);
  const label = isBot ? '[B]' : '[H]';
  console.log(`${label} ${request.url} IP=${ip} UA=${ua}`);

  // 5. Amazon偽装 Bot 対策（従来通り）
  if (!ua.includes('AmazonProductDiscovery/1.0')) {
    return fetch(request);
  }

  const cidrs = await env.BOT_BLOCKER_KV.get('AMAZON_IPS', { type: 'json', cacheTtl: 3600 }) || [];
  if (!Array.isArray(cidrs) || cidrs.length === 0) {
    console.log('[WARN] AMAZON_IPS empty -> bypass');
    return fetch(request);
  }
  if (!cidrs.some(c => ipInCidr(ip, c))) {
    console.log(`[BLOCKED] ${request.url} IP=${ip} UA=${ua}`);
    return new Response('Not Found', { status: 404 });
  }
  console.log(`[ALLOWED] ${request.url} IP=${ip} UA=${ua}`);
  return fetch(request);
}

/* ---- ユーティリティ ---- */

// ロケール抽出: /en-us/xxx → en-us、/products → root
function extractLocale(path) {
  // Shopify の多言語構成に合わせて: /xx/, /xx-xx/
  const seg = path.split('/').filter(Boolean)[0];
  if (!seg) return 'root';
  // 2文字 or 2+2 のパターン程度をロケールとみなす
  if (/^[a-z]{2}(-[a-z]{2})?$/.test(seg)) return seg;
  return 'root';
}

function ipInCidr(ip, cidr) {
  if (ip.includes(':')) return false; // IPv6は未対応
  const [base, bits = '32'] = cidr.split('/');
  const mask = (-1 << (32 - Number(bits))) >>> 0;
  return (toInt(ip) & mask) === (toInt(base) & mask);
}
const toInt = s => s.split('.').reduce((a, o) => (a << 8) + +o, 0) >>> 0;

// ====== ロケールファンアウト KV ======
const LOCALE_WINDOW = 30 * 1000;        // 30秒
const LOCALE_THRESHOLD = 3;             // 3ロケール以上
const BLOCK_DURATION = 24 * 3600 * 1000;

async function localeFanoutCheck(ip, locale, env) {
  if (!ip) return { allow: true };
  const now = Date.now();
  const raw = await env.LOCALE_FANOUT.get(ip);
  let data = raw ? JSON.parse(raw) : { locales: {}, blockedUntil: 0 };

  if (data.blockedUntil > now) {
    return { allow: false, reason: 'still-blocked' };
  }

  // 古い記録掃除
  for (const [l, t] of Object.entries(data.locales)) {
    if (now - t > LOCALE_WINDOW) delete data.locales[l];
  }
  data.locales[locale] = now;

  const fanout = Object.keys(data.locales).length;
  if (fanout >= LOCALE_THRESHOLD) {
    data.blockedUntil = now + BLOCK_DURATION;
    await env.LOCALE_FANOUT.put(ip, JSON.stringify(data), { expirationTtl: 2 * 24 * 3600 });
    return { allow: false, reason: `locale-fanout(${fanout})` };
  } else {
    await env.LOCALE_FANOUT.put(ip, JSON.stringify(data), { expirationTtl: 24 * 3600 });
    return { allow: true };
  }
}
