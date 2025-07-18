/**
 * Cloudflare Worker - 高度なBot対策とアクセス制御
 *
 * 機能:
 * 1. 静的アセットへのアクセスを無視
 * 2. 多言語サイトへのクローリング攻撃（ロケールファンアウト）を検知し、一時的にブロック
 * 3. 開発者自身のアクセスをCookieで判定し、ブロックチェックから除外
 * 4. User-Agentに基づき、アクセスをHuman[H], Bot[B], Service[S]に分類してロギング
 * 5. AmazonProductDiscovery/1.0 を名乗る偽装Botを、正規IP以外からのアクセスの場合のみブロック
 * 6. IP/CIDR判定はIPv4とIPv6の両方に対応
 */

// --- エントリポイント ---

export default {
  async fetch(request, env, ctx) {
    // メインの処理関数を呼び出す
    return handle(request, env);
  }
};

// --- メインの処理関数 ---

async function handle(request, env) {
  const ua = request.headers.get('User-Agent') || 'UA_NOT_FOUND';
  const ip = request.headers.get('CF-Connecting-IP') || 'IP_NOT_FOUND';
  const cookie = request.headers.get('Cookie') || '';
  const { pathname } = new URL(request.url);
  const path = pathname.toLowerCase();

  // 1. 静的アセットやShopify内部APIは処理対象外
  const EXT_SKIP = /\.(jpg|jpeg|png|gif|svg|webp|js|css|woff2?|ttf|ico|map|txt|eot|otf|json|xml|avif)(\?|$)/i;
  const PATH_SKIP = path.startsWith('/wpm@') ||
                    path.includes('/cart.js') ||
                    path.includes('/recommendations/') ||
                    path.startsWith('/_t/');
  if (EXT_SKIP.test(path) || PATH_SKIP) {
    return fetch(request);
  }
  
  // 2. 開発者自身のアクセスを除外（ロケールファンアウトチェック）
  // "bypass_fanout=true" というCookieがあれば、チェックをスキップ
  if (!cookie.includes('bypass_fanout=true')) {
    const locale = extractLocale(path);
    const fanout = await localeFanoutCheck(ip, locale, env);
    if (!fanout.allow) {
      console.log(`[LF-BLOCK] ${request.url} IP=${ip} UA=${ua} reason=${fanout.reason || 'locale-fanout'}`);
      return new Response('Not Found', { status: 404 });
    }
  }

  // 3. アクセス元を分類してロギング
  const servicePattern = /(python-requests|aiohttp|monitor|insights)/i;
  const botPattern = /(bot|crawl|spider|slurp|fetch|headless|preview|externalagent|barkrowler|bingbot|petalbot)/i;
  let label;
  if (servicePattern.test(ua)) {
    label = '[S]'; // Service / API
  } else if (botPattern.test(ua)) {
    label = '[B]'; // Bot
  } else {
    label = '[H]'; // Human
  }
  console.log(`${label} ${request.url} IP=${ip} UA=${ua}`);

  // 4. Amazon偽装Bot対策
  if (ua.includes('AmazonProductDiscovery/1.0')) {
    const cidrs = await env.BOT_BLOCKER_KV.get('AMAZON_IPS', { type: 'json', cacheTtl: 3600 }) || [];
    if (!Array.isArray(cidrs) || cidrs.length === 0) {
      console.log('[WARN] AMAZON_IPS in KV is empty or invalid. Bypassing Amazon bot check.');
      return fetch(request);
    }
    if (!cidrs.some(c => ipInCidr(ip, c))) {
      console.log(`[BLOCKED] Fake AmazonProductDiscovery bot. IP=${ip} UA=${ua}`);
      return new Response('Not Found', { status: 404 });
    }
  }
  
  return fetch(request);
}

// --- ユーティリティ関数 ---

/**
 * パスからロケールを抽出します (/ja/products -> ja)
 * @param {string} path - URLのパス
 * @returns {string} ロケール文字列、または 'root'
 */
function extractLocale(path) {
  const seg = path.split('/').filter(Boolean)[0];
  if (!seg) return 'root';
  // Shopifyの一般的なロケール形式 /xx/ または /xx-XX/ に一致
  if (/^[a-z]{2}(-[a-z]{2})?$/.test(seg)) return seg;
  return 'root';
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
      return false; // 不正なプレフィックス
    }

    const ipBigInt = ipToBigInt(ip);
    const baseBigInt = ipToBigInt(base);
    
    // ビットマスクを作成
    const mask = ((1n << BigInt(prefix)) - 1n) << BigInt(totalBits - prefix);
    
    return (ipBigInt & mask) === (baseBigInt & mask);
  } catch (e) {
    console.error(`[ipInCidr] Error: ip='${ip}' cidr='${cidr}'`, e);
    return false;
  }
}

/**
 * IPアドレス（v4/v6）をBigIntに変換します。
 * @param {string} ip - IPアドレス
 * @returns {BigInt}
 */
function ipToBigInt(ip) {
  if (ip.includes(':')) {
    // IPv6
    // '::' を展開して8つの16進数グループに正規化
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
    
    // BigIntに変換
    return fullIp.reduce((acc, part) => {
      const value = part ? BigInt(`0x${part}`) : 0n;
      return (acc << 16n) + value;
    }, 0n);

  } else {
    // IPv4
    return ip.split('.').reduce((acc, part) => (acc << 8n) + BigInt(part), 0n);
  }
}


// --- ロケールファンアウトKV関連 ---

const LOCALE_WINDOW = 30 * 1000;      // 30秒
const LOCALE_THRESHOLD = 3;           // 3ロケール以上でブロック
const BLOCK_DURATION = 24 * 3600 * 1000; // 24時間

async function localeFanoutCheck(ip, locale, env) {
  if (!ip || !env.LOCALE_FANOUT) return { allow: true }; // KVが設定されていなければ何もしない
  
  const now = Date.now();
  const raw = await env.LOCALE_FANOUT.get(ip);
  let data = raw ? JSON.parse(raw) : { locales: {}, blockedUntil: 0 };

  if (data.blockedUntil > now) {
    return { allow: false, reason: 'still-blocked' };
  }

  // 期間外の古い記録を掃除
  for (const [l, t] of Object.entries(data.locales)) {
    if (now - t > LOCALE_WINDOW) delete data.locales[l];
  }
  data.locales[locale] = now;

  const fanout = Object.keys(data.locales).length;
  if (fanout >= LOCALE_THRESHOLD) {
    data.blockedUntil = now + BLOCK_DURATION;
    // ブロック期間（24h）より少し長くKVエントリを保持（48h）
    await env.LOCALE_FANOUT.put(ip, JSON.stringify(data), { expirationTtl: 2 * 24 * 3600 });
    return { allow: false, reason: `locale-fanout(${fanout})` };
  } else {
    // 最終アクセスから24hでKVから消去されるように設定
    await env.LOCALE_FANOUT.put(ip, JSON.stringify(data), { expirationTtl: 24 * 3600 });
    return { allow: true };
  }
}
