/*
 * =================================================================
 * 目次 (Table of Contents)
 * =================================================================
 * 1. logAndBlock:           ブロック処理とログ記録を共通化する関数
 * 2. countViolation:        Durable Object を使って IP の違反回数をカウントし取得する関数
 * 3. handle:                メイン処理関数 (リクエストハンドラ)
 * 4. extractLocale:         パスからロケールを抽出する関数
 * 5. ipToBigInt:            IPアドレスを BigInt に変換する関数 (IPv4/v6対応)
 * 6. ipInCidr:              IPアドレスが CIDR 範囲内かチェックする関数 (IPv4/v6対応)
 * 7. localeFanoutCheck:     振る舞い検知・段階的ブロックを行う関数
 *
 * =================================================================
 * 便利なコマンド (Useful Commands)
 * =================================================================
 *
 * H判定（人間）のログをリアルタイム表示:
 * npx wrangler tail shopify-bot-blocker --format=pretty | grep "[H]"
 *
 * B判定（ボット）のログをリアルタイム表示:
 * npx wrangler tail shopify-bot-blocker --format=pretty | grep "[B]"
 *
 * ブロックされたIPのログ一覧を表示:
 * npx wrangler kv:key list --binding=LOCALE_FANOUT --prefix="blocked:"
 *
 * =================================================================
 */

export default {
  async fetch(request, env, ctx) {
    return handle(request, env, ctx);
  }
};

// 1. ブロック処理とログ記録を共通化する関数
async function logAndBlock(ip, ua, reason, env, ctx) {
  console.log(`[BLOCKED] IP=${ip} UA=${ua} reason=${reason}`);

  const now = new Date();
  const logKey = `blocked:${reason}:${ip}`;
  const logValue = JSON.stringify({
    timestamp: now.toISOString(),
    userAgent: ua,
  });
  const thirtyDays = 30 * 24 * 3600;

  ctx.waitUntil(env.LOCALE_FANOUT.put(logKey, logValue, { expirationTtl: thirtyDays }));
  return new Response("Not Found", { status: 404 });
}

// 2. Durable Object を使って違反回数をカウント・取得する関数
async function countViolation(ip, env) {
  const id = env.IP_COUNTER.idFromName(ip);
  const stub = env.IP_COUNTER.get(id);
  const res = await stub.fetch(new Request("https://internal/", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ip }),
  }));
  if (!res.ok) {
    console.error(`DO countViolation failed for IP=${ip}`);
    return { ip, count: 0 };
  }
  return res.json(); // { ip, count }
}

// 3. メイン処理関数（リクエストハンドラ）
async function handle(request, env, ctx) {
  const ua = request.headers.get("User-Agent") || "UA_NOT_FOUND";
  const ip = request.headers.get("CF-Connecting-IP") || "IP_NOT_FOUND";
  const { pathname } = new URL(request.url);
  const path = pathname.toLowerCase();

  // --- まずは違反カウントの更新／取得 ---
  const { count } = await countViolation(ip, env);

  // --- 違反回数に応じた即時 KV 操作 ---
  if (count === 1) {
    await env.BOT_BLOCKER_KV.put(ip, "temp-1", { expirationTtl: 600 });
  } else if (count === 2) {
    await env.BOT_BLOCKER_KV.put(ip, "temp-2", { expirationTtl: 600 });
  } else if (count >= 3) {
    await env.BOT_BLOCKER_KV.put(ip, "permanent-block");
    // R2 に永続化
    const record = JSON.stringify({
      ip,
      reason: "violation-count",
      count,
      timestamp: new Date().toISOString(),
    });
    ctx.waitUntil(env.BLOCKLIST_R2.put(`${ip}.json`, record));
  }

  // --- 既存 block 判定用 KV チェック ---
  const status = await env.BOT_BLOCKER_KV.get(ip);
  if (status === "temp-1" || status === "temp-2") {
    return new Response("Not Found", { status: 404 });
  }
  if (status === "permanent-block") {
    return new Response("Not Found", { status: 404 });
  }

  // 以下は既存ロジック（UA判定や localeFanout など）を省略なく継続実行
  const staticBlockIps = new Set([]);
  for (const block of staticBlockIps) {
    if (ipInCidr(ip, block)) {
      return logAndBlock(ip, ua, "static-ip", env, ctx);
    }
  }
  if (path.includes("/wp-") || path.endsWith(".php") || path.includes("/phpmyadmin")
    || path.endsWith("/.env") || path.endsWith("/config") || path.includes("/admin/")
    || path.includes("/dbadmin")) {
    return logAndBlock(ip, ua, "path-scan", env, ctx);
  }

  const locale = extractLocale(path);
  const EXT_SKIP = /\.(jpg|jpeg|png|gif|svg|webp|js|css|woff2?|ttf|ico|map|txt|eot|otf|json|xml|avif)(\?|$)/;
  const PATH_SKIP = path.startsWith("/wpm@") || path.includes("/cart.js")
    || path.includes("/recommendations/") || path.startsWith("/_t/");
  if (EXT_SKIP.test(path) || PATH_SKIP) return fetch(request);

  const servicePattern = /(python-requests|aiohttp|monitor|insights)/i;
  const botPattern = /(bot|crawl|spider|slurp|fetch|headless|preview|externalagent|barkrowler|bingbot|petalbot)/i;

  let label;
  if (servicePattern.test(ua)) {
    label = "[S]";
  } else if (botPattern.test(ua)) {
    label = "[B]";
  } else {
    label = "[H]";
  }
  console.log(`${label} ${request.url} IP=${ip} UA=${ua}`);

  if (label === "[B]" && unwantedBotPatterns === null) {
    try {
      const patternsJson = await env.LOCALE_FANOUT.get("UNWANTED_BOT_UA_PATTERNS");
      unwantedBotPatterns = patternsJson ? JSON.parse(patternsJson) : [];
    } catch {
      unwantedBotPatterns = [];
    }
  }
  if (label === "[B]" && unwantedBotPatterns.length > 0) {
    for (const patt of unwantedBotPatterns) {
      try {
        if (new RegExp(patt, "i").test(ua)) {
          return logAndBlock(ip, ua, `unwanted-bot:${patt}`, env, ctx);
        }
      } catch {}
    }
  }

  if (label === "[H]") {
    const fanout = await localeFanoutCheck(ip, locale, ua, env, ctx);
    if (!fanout.allow) {
      return logAndBlock(ip, ua, fanout.reason, env, ctx);
    }
  }

  if (ua.startsWith("AmazonProductDiscovery/1.0")) {
    const cidrs = await env.BOT_BLOCKER_KV.get("AMAZON_IPS", { type: "json", cacheTtl: 3600 }) || [];
    if (!cidrs.some(c => ipInCidr(ip, c))) {
      return logAndBlock(ip, ua, "amazon-impersonation", env, ctx);
    }
  }

  return fetch(request);
}

// 4. パスからロケールを抽出する関数
function extractLocale(path) {
  const seg = path.split('/').filter(Boolean)[0];
  if (!seg) return 'root';
  if (/^[a-z]{2}(-[a-z]{2})?$/.test(seg)) return seg;
  return 'root';
}

// 5. IPアドレスをBigIntに変換する関数 (IPv4/v6対応)
function ipToBigInt(ip) {
  if (ip.includes(':')) {
    // IPv6 対応
    const parts = ip.split('::');
    let part1 = [], part2 = [];
    if (parts.length > 1) {
      part1 = parts[0].split(':').filter(p => p.length > 0);
      part2 = parts[1].split(':').filter(p => p.length > 0);
    } else {
      part1 = ip.split(':');
    }
    const zeroGroups = 8 - (part1.length + part2.length);
    const full = [...part1, ...Array(zeroGroups).fill('0'), ...part2];
    return full.reduce((acc, p) => (acc << 16n) + BigInt(`0x${p}`), 0n);
  } else {
    // IPv4
    return ip.split('.').reduce((acc, p) => (acc << 8n) + BigInt(p), 0n);
  }
}

// 6. IPアドレスがCIDR範囲内かチェックする関数 (IPv4/v6対応)
function ipInCidr(ip, cidr) {
  try {
    const isV6 = cidr.includes(':');
    const totalBits = isV6 ? 128 : 32;
    const [base, prefixStr] = cidr.split('/');
    const prefix = parseInt(prefixStr, 10);
    if (isNaN(prefix) || prefix < 0 || prefix > totalBits) return false;
    const ipVal = ipToBigInt(ip);
    const baseVal = ipToBigInt(base);
    const mask = ((1n << BigInt(prefix)) - 1n) << BigInt(totalBits - prefix);
    return (ipVal & mask) === (baseVal & mask);
  } catch (e) {
    console.error(`[ipInCidr] Error: ip='${ip}' cidr='${cidr}'`, e);
    return false;
  }
}

// 7. 振る舞い検知・段階的ブロックを行う関数
const LOCALE_WINDOW = 30 * 1000;
const LOCALE_THRESHOLD = 3;

async function localeFanoutCheck(ip, locale, ua, env, ctx) {
  if (!ip) return { allow: true };
  const now = Date.now();

  let data;
  try {
    const raw = await env.LOCALE_FANOUT.get(ip);
    data = raw ? JSON.parse(raw) : { locales: {}, blockedUntil: 0, offenseCount: 0 };
  } catch (e) {
    console.error("localeFanoutCheck: KV read/parse error", e);
    data = { locales: {}, blockedUntil: 0, offenseCount: 0 };
  }

  if (data.blockedUntil > now) {
    return { allow: false, reason: `still-blocked:offense-${data.offenseCount}` };
  }

  let needsWrite = false;
  for (const [loc, ts] of Object.entries(data.locales)) {
    if (now - ts > LOCALE_WINDOW) {
      delete data.locales[loc];
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
        blockDuration = 10 * 60 * 1000; break;
      case 2:
        blockDuration = 24 * 3600 * 1000; break;
      default:
        blockDuration = 7 * 24 * 3600 * 1000; break;
    }
    const reason = `locale-fanout:offense-${data.offenseCount}`;
    data.blockedUntil = now + blockDuration;
    ctx.waitUntil(env.LOCALE_FANOUT.put(ip, JSON.stringify(data), { expirationTtl: 8 * 24 * 3600 }));
    return { allow: false, reason };
  }

  if (needsWrite) {
    ctx.waitUntil(env.LOCALE_FANOUT.put(ip, JSON.stringify(data), { expirationTtl: 24 * 3600 }));
  }

  return { allow: true };
}
