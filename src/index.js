export default {
  async fetch(request, env, ctx) {
    // handle関数にenvとctxを渡す
    return handle(request, env, ctx);
  }
};

async function handle(request, env, ctx) {
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
    label = '[S]';
  } else if (botPattern.test(ua)) {
    label = '[B]';
  } else {
    label = '[H]';
  }
  console.log(`${label} ${request.url} IP=${ip} UA=${ua}`);
  
  // 4. ロケールファンアウトチェック (H判定のみ)
  if (label === '[H]') {
    // ctxを渡してwaitUntilを使えるようにする
    const fanout = await localeFanoutCheck(ip, locale, ua, env, ctx);
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

function extractLocale(path) {
  const seg = path.split('/').filter(Boolean)[0];
  if (!seg) return 'root';
  if (/^[a-z]{2}(-[a-z]{2})?$/.test(seg)) return seg;
  return 'root';
}

function ipToBigInt(ip) {
  if (ip.includes(':')) {
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
    return fullIp.reduce((acc, part) => (acc << 16n) + (part ? BigInt(`0x${part}`) : 0n), 0n);
  } else {
    return ip.split('.').reduce((acc, part) => (acc << 8n) + BigInt(part), 0n);
  }
}

function ipInCidr(ip, cidr) {
  try {
    const isIPv6 = cidr.includes(':');
    const totalBits = isIPv6 ? 128 : 32;
    const [base, bitsStr] = cidr.split('/');
    const prefix = parseInt(bitsStr, 10);
    if (isNaN(prefix) || prefix < 0 || prefix > totalBits) return false;
    const ipBigInt = ipToBigInt(ip);
    const baseBigInt = ipToBigInt(base);
    const mask = ((1n << BigInt(prefix)) - 1n) << BigInt(totalBits - prefix);
    return (ipBigInt & mask) === (baseBigInt & mask);
  } catch (e) {
    console.error(`[ipInCidr] Error: ip='${ip}' cidr='${cidr}'`, e);
    return false;
  }
}

const LOCALE_WINDOW = 30 * 1000;
const LOCALE_THRESHOLD = 3;

// ★★★ここからが変更ブロック★★★
async function localeFanoutCheck(ip, locale, ua, env, ctx) {
  if (!ip) return { allow: true };

  const now = Date.now();
  const raw = await env.LOCALE_FANOUT.get(ip);
  // offenseCountを追加
  let data = raw ? JSON.parse(raw) : { locales: {}, blockedUntil: 0, offenseCount: 0 };
  
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
    // 違反回数をインクリメント
    data.offenseCount = (data.offenseCount || 0) + 1;
    
    // 違反回数に応じてブロック期間を決定
    const isRepeatOffender = data.offenseCount > 1;
    const blockDuration = isRepeatOffender ? (24 * 3600 * 1000) : (10 * 60 * 1000); // 再犯なら24h、初回なら10m
    const reason = `locale-fanout(${fanout})${isRepeatOffender ? '-repeat' : ''}`;
    
    data.blockedUntil = now + blockDuration;
    needsWrite = true;
    
    // ブロックログをKVに保存
    const logKey = `blocked_ip:${ip}`;
    const logValue = JSON.stringify({
      timestamp: new Date(now).toISOString(),
      reason: reason,
      offenseCount: data.offenseCount,
      userAgent: ua,
    });
    const thirtyDays = 30 * 24 * 3600;
    ctx.waitUntil(env.LOCALE_FANOUT.put(logKey, logValue, { expirationTtl: thirtyDays }));
    
    if (needsWrite) {
      await env.LOCALE_FANOUT.put(ip, JSON.stringify(data), { expirationTtl: 2 * 24 * 3600 });
    }
    return { allow: false, reason: reason };
  }
  
  if (needsWrite) {
    await env.LOCALE_FANOUT.put(ip, JSON.stringify(data), { expirationTtl: 24 * 3600 });
  }

  return { allow: true };
}
// ★★★ここまで★★★
