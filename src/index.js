export default {
  async fetch(request, env, ctx) {
    // handle関数を呼び出し、envオブジェクトを渡す
    return handle(request, env);
  }
};

async function handle(request, env) {
  const ua = request.headers.get('User-Agent') || 'UA_NOT_FOUND';
  const ip = request.headers.get('CF-Connecting-IP') || 'IP_NOT_FOUND';
  const { pathname } = new URL(request.url);
  const path = pathname.toLowerCase();

  // 1. ロケール抽出
  const locale = extractLocale(path);

  // 2. 静的リソースはスキップ
  const EXT_SKIP = /\.(jpg|jpeg|png|gif|svg|webp|js|css|woff2?|ttf|ico|map|txt|eot|otf|json|xml|avif)(\?|$)/;
  const PATH_SKIP = path.startsWith('/wpm@') ||
                    path.includes('/cart.js') ||
                    path.includes('/recommendations/') ||
                    path.startsWith('/_t/');
  if (EXT_SKIP.test(path) || PATH_SKIP) return fetch(request);

  // 3. Bot / Service ラベル付与
  const servicePattern = /(python-requests|aiohttp|monitor|insights)/i;
  const botPattern = /(bot|crawl|spider|slurp|fetch|headless|preview|externalagent|barkrowler|bingbot|petalbot)/i;

  let label;
  if (servicePattern.test(ua)) {
    label = '[S]'; // Service
  } else if (botPattern.test(ua)) {
    label = '[B]'; // Bot
  } else {
    label = '[H]'; // Human
  }
  console.log(`${label} ${request.url} IP=${ip} UA=${ua}`);
  
  // 4. ロケールファンアウトチェック (H判定のみ)
  if (label === '[H]') {
    const fanout = await localeFanoutCheck(ip, locale, env);
    if (!fanout.allow) {
      console.log(`[LF-BLOCK] ${request.url} IP=${ip} UA=${ua} reason=${fanout.reason||'locale-fanout'}`);
      return new Response('Not Found', { status: 404 });
    }
  }

  // 5. Amazon偽装 Bot 対策
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
  const seg = path.split('/').filter(Boolean)[0];
  if (!seg) return 'root';
  if (/^[a-z]{2}(-[a-z]{2})?$/.test(seg)) return seg;
  return 'root';
}

/**
 * IPアドレス（v4/v6）をBigIntに変換します。
 * @param {string} ip - IPアドレス
 * @returns {BigInt}
 */
function ipToBigInt(ip) {
  if (ip.includes(':')) {
    // IPv6
    const parts = ip.split('::');
    let part1 = [], part2 = [];
    if (parts.length > 1) {
      part1 = parts[0].split(':').filter(p => p.length > 0);
      part2 = parts[1].split(':').filter(p => p.length > 0);
    } else {
      part1 = ip.split(':');
    }
    const zeroGroups = 8 - (part1.length + part2.length);
    const fullIp = [...part1, ...Array(zeroGroups).fill('0'), ...part2];
    
    return fullIp.reduce((acc, part) => {
      const value = part ? BigInt(`0x${part}`) : 0n;
      return (acc << 16n) + value;
    }, 0n);
  } else {
    // IPv4
    return ip.split('.').reduce((acc, part) => (acc << 8n) + BigInt(part), 0n);
  }
}

/**
 * IPアドレスがCIDR範囲内にあるかチェックします（IPv4/IPv6対応）。
 * @param {string} ip - チェックするIPアドレス
 * @param {string} cidr - CIDR範囲
 * @returns {boolean}
 */
function ipInCidr(ip, cidr) {
  try {
    const isIPv6 = cidr.includes(':');
    const totalBits = isIPv6 ? 128 : 32;
    
    const [base, bitsStr] = cidr.split('/');
    const prefix = parseInt(bitsStr, 10);
    
    if (isNaN(prefix) || prefix < 0 || prefix > totalBits) {
      return false;
    }

    const ipBigInt = ipToBigInt(ip);
    const baseBigInt = ipToBigInt(base);
    
    const mask = ( (1n << BigInt(prefix)) - 1n ) << BigInt(totalBits - prefix);
    
    return (ipBigInt & mask) === (baseBigInt & mask);
  } catch (e) {
    console.error(`[ipInCidr] Error: ip='${ip}' cidr='${cidr}'`, e);
    return false;
  }
}

// ====== ロケールファンアウト KV ======
const LOCALE_WINDOW = 30 * 1000;      // 30秒
const LOCALE_THRESHOLD = 3;           // 3ロケール以上
const BLOCK_DURATION = 24 * 3600 * 1000; // 24時間

async function localeFanoutCheck(ip, locale, env) {
  if (!ip) return { allow: true };

  const now = Date.now();
  const raw = await env.LOCALE_FANOUT.get(ip);
  let data = raw ? JSON.parse(raw) : { locales: {}, blockedUntil: 0 };
  
  let needsWrite = false;

  if (data.blockedUntil > now) {
    return { allow: false, reason: 'still-blocked' };
  }

  // 古い記録掃除
  for (const [l, t] of Object.entries(data.locales)) {
    if (now - t > LOCALE_WINDOW) {
      delete data.locales[l];
      needsWrite = true;
    }
  }
  
  if (!data.locales[locale]) {
    needsWrite = true;
  }
  data.locales[locale] = now;

  const fanout = Object.keys(data.locales).length;
  if (fanout >= LOCALE_THRESHOLD) {
    data.blockedUntil = now + BLOCK_DURATION;
    needsWrite = true;
    
    if(needsWrite) {
      await env.LOCALE_FANOUT.put(ip, JSON.stringify(data), { expirationTtl: 2 * 24 * 3600 });
    }
    return { allow: false, reason: `locale-fanout(${fanout})` };
  }
  
  if (needsWrite) {
    await env.LOCALE_FANOUT.put(ip, JSON.stringify(data), { expirationTtl: 24 * 3600 });
  }

  return { allow: true };
}
