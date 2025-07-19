/*
 * =================================================================
 * 目次 (Table of Contents)
 * =================================================================
 * 1. logAndBlock:           ブロック処理とログ記録を共通化する関数
 * 2. handle:               メイン処理関数 (リクエストハンドラ)
 * 3. extractLocale:        パスからロケールを抽出する関数
 * 4. ipToBigInt:           IPアドレスをBigIntに変換する関数 (IPv4/v6対応)
 * 5. ipInCidr:             IPアドレスがCIDR範囲内かチェックする関数 (IPv4/v6対応)
 * 6. localeFanoutCheck:    振る舞い検知・段階的ブロックを行う関数
 *
 * =================================================================
 * 便利なコマンド (Useful Commands)
 * =================================================================
 *
 * H判定（人間）のログをリアルタイム表示:
 * npx wrangler tail shopify-bot-blocker --format=pretty | grep "\[H\]"
 *
 * B判定（ボット）のログをリアルタイム表示:
 * npx wrangler tail shopify-bot-blocker --format=pretty | grep "\[B\]"
 *
 * ブロックされたIPのログ一覧を表示 (プレフィックスで絞り込み):
 * ※ `LOCALE_FANOUT` は実際のKV名前空間バインディング名に置き換えてください
 * npx wrangler kv:key list --binding=LOCALE_FANOUT --prefix="blocked:"
 *
 * =================================================================
 */


export default {
  async fetch(request, env, ctx) {
    return handle(request, env, ctx);
  }
};

// =================================================================
// 1. ブロック処理とログ記録を共通化する関数
// =================================================================
async function logAndBlock(ip, ua, reason, env, ctx) {
  console.log(`[BLOCKED] IP=${ip} UA=${ua} reason=${reason}`);
  
  const now = new Date();
  // ★改善点: ログキーに reason を含める
  const logKey = `blocked:${reason}:${ip}`;
  const logValue = JSON.stringify({
    timestamp: now.toISOString(),
    userAgent: ua,
  });
  const thirtyDays = 30 * 24 * 3600;

  ctx.waitUntil(env.LOCALE_FANOUT.put(logKey, logValue, { expirationTtl: thirtyDays }));
  
  return new Response('Not Found', { status: 404 });
}

// KVから読み込んだブロックリストをキャッシュする変数
let unwantedBotPatterns = null;
// =================================================================
// 2. メイン処理関数 (リクエストハンドラ)
// =================================================================
async function handle(request, env, ctx) {
  const ua = request.headers.get('User-Agent') || 'UA_NOT_FOUND';
  const ip = request.headers.get('CF-Connecting-IP') || 'IP_NOT_FOUND';
  const { pathname } = new URL(request.url);
  const path = pathname.toLowerCase();

  // --- KVから不要ボットリストを読み込み＆キャッシュ ---
  if (unwantedBotPatterns === null) {
    try {
      console.log('Fetching unwanted bot patterns from KV...');
      const patternsJson = await env.LOCALE_FANOUT.get('UNWANTED_BOT_UA_PATTERNS');
      unwantedBotPatterns = patternsJson ? JSON.parse(patternsJson) : [];
      console.log(`Loaded ${unwantedBotPatterns.length} unwanted bot patterns.`);
    } catch (e) {
      console.error("Failed to load or parse UNWANTED_BOT_UA_PATTERNS from KV", e);
      unwantedBotPatterns = []; // エラー時は空のリストとして処理を続行
    }
  }
  
  // --- 静的ルールによるブロック ---
  const staticBlockIps = new Set([
    // 永久追放したいIPのみ、手動でここに追加
  ]);
  
  for (const block of staticBlockIps) {
    if (ipInCidr(ip, block)) {
      return logAndBlock(ip, ua, 'static-ip', env, ctx);
    }
  }

  const isPathBlocked = path.includes('/wp-') ||
                        path.endsWith('.php') ||
                        path.includes('/phpmyadmin') ||
                        path.endsWith('/.env') ||
                        path.endsWith('/config') ||
                        path.includes('/admin/') ||
                        path.includes('/dbadmin');

  if (isPathBlocked) {
    return logAndBlock(ip, ua, 'path-scan', env, ctx);
  }

  // --- 基本処理 ---
  const locale = extractLocale(path);
  const EXT_SKIP = /\.(jpg|jpeg|png|gif|svg|webp|js|css|woff2?|ttf|ico|map|txt|eot|otf|json|xml|avif)(\?|$)/;
  const PATH_SKIP = path.startsWith('/wpm@') ||
                    path.includes('/cart.js') ||
                    path.includes('/recommendations/') ||
                    path.startsWith('/_t/');
  if (EXT_SKIP.test(path) || PATH_SKIP) return fetch(request);

  // --- トラフィック分類 ---
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

  // --- 不要なボットのブロック ---
  if (label === '[B]' && unwantedBotPatterns.length > 0) {
    for (const pattern of unwantedBotPatterns) {
      try {
        if (new RegExp(pattern, 'i').test(ua)) {
          return logAndBlock(ip, ua, `unwanted-bot:${pattern}`, env, ctx);
        }
      } catch (e) {
        // 正規表現が無効な場合に備える
        console.error(`Invalid regex pattern in blocklist: ${pattern}`, e);
      }
    }
  }

  // --- 動的ルールによるブロック (振る舞い検知) ---
  if (label === '[H]') {
    const fanout = await localeFanoutCheck(ip, locale, ua, env, ctx);
    if (!fanout.allow) {
      return logAndBlock(ip, ua, fanout.reason, env, ctx);
    }
  }

  // --- 特定Botへの対策 ---
  if (ua.startsWith('AmazonProductDiscovery/1.0')) {
    const cidrs = await env.BOT_BLOCKER_KV.get('AMAZON_IPS', { type: 'json', cacheTtl: 3600 }) || [];
    if (!Array.isArray(cidrs) || cidrs.length === 0) {
      console.log('[WARN] AMAZON_IPS empty -> bypass');
    } else if (!cidrs.some(c => ipInCidr(ip, c))) {
      return logAndBlock(ip, ua, 'amazon-impersonation', env, ctx);
    } else {
      console.log(`[ALLOWED] AmazonBot ${request.url} IP=${ip}`);
    }
  }

  // すべてのチェックを通過したリクエスト
  return fetch(request);
}

// =================================================================
// 3. パスからロケールを抽出する関数 (変更なし)
// =================================================================
function extractLocale(path) {
  const seg = path.split('/').filter(Boolean)[0];
  if (!seg) return 'root';
  if (/^[a-z]{2}(-[a-z]{2})?$/.test(seg)) return seg;
  return 'root';
}

// =================================================================
// 4. IPアドレスをBigIntに変換する関数 (変更なし)
// =================================================================
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

// =================================================================
// 5. IPアドレスがCIDR範囲内かチェックする関数 (変更なし)
// =================================================================
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

// =================================================================
// 6. 振る舞い検知・段階的ブロックを行う関数 (変更なし)
// =================================================================
const LOCALE_WINDOW = 30 * 1000;
const LOCALE_THRESHOLD = 3;

async function localeFanoutCheck(ip, locale, ua, env, ctx) {
  if (!ip) return { allow: true };

  const now = Date.now();
  const raw = await env.LOCALE_FANOUT.get(ip);
  let data = raw ? JSON.parse(raw) : { locales: {}, blockedUntil: 0, offenseCount: 0 };
  
  if (data.blockedUntil > now) {
    return { allow: false, reason: `still-blocked:offense-${data.offenseCount}` };
  }

  let needsWrite = false;

  for (const [l, t] of Object.entries(data.locales)) {
    if (now - t > LOCALE_WINDOW) {
      delete data.locales[l];
      needsWrite = true;
    }
  }
  
  if (!data.locales[locale]) {
    data.locales[locale] = now;
    needsWrite = true;
  } else {
    data.locales[locale] = now;
  }

  const fanout = Object.keys(data.locales).length;
  if (fanout >= LOCALE_THRESHOLD) {
    data.offenseCount = (data.offenseCount || 0) + 1;
    
    let blockDuration;
    switch (data.offenseCount) {
      case 1:
        blockDuration = 10 * 60 * 1000; // 10分
        break;
      case 2:
        blockDuration = 24 * 3600 * 1000; // 24時間
        break;
      default:
        blockDuration = 7 * 24 * 3600 * 1000; // 7日間
        break;
    }
    
    const reason = `locale-fanout:offense-${data.offenseCount}`;
    data.blockedUntil = now + blockDuration;
    
    await env.LOCALE_FANOUT.put(ip, JSON.stringify(data), { expirationTtl: 8 * 24 * 3600 });
    
    return { allow: false, reason: reason };
  }
  
  if (needsWrite) {
     await env.LOCALE_FANOUT.put(ip, JSON.stringify(data), { expirationTtl: 24 * 3600 });
  }

  return { allow: true };
}
