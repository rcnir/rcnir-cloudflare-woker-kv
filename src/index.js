/*
 * =================================================================
 * 目次 (Table of Contents)
 * =================================================================
 * 1. エクスポートとメインハンドラ (Exports & Main Handlers)
 * - Durable Objectの再エクスポート
 * - `default.fetch`: リクエスト毎の処理
 * - `default.scheduled`: Cron Triggerによる定時処理
 * 2. メインロジック (Main Logic)
 * - `handle`: 全リクエストを処理するコア関数
 * 3. コアヘルパー関数 (Core Helper Functions)
 * - `handleViolation`: 違反を検知し、DO/KV/R2を操作する中心関数
 * - `verifyBotIp`: `BOT_CIDRS` KVを使い、正規ボットのIPを検証する関数
 * - `logAndBlock`: 違反カウント対象外の即時ブロックとログ記録を行う関数
 * 4. ユーティリティ関数 (Utility Functions)
 * - `extractLocale`: パスからロケールを抽出する関数
 * - `ipToBigInt`: IPアドレスをBigIntに変換する関数
 * - `ipInCidr`: IPがCIDR範囲内かチェックする関数
 * - `localeFanoutCheck`: 短時間の多ロケールアクセスを検知する関数
 * =================================================================
 */

// --- 1. エクスポートとメインハンドラ ---

import { IPBlockCounter } from "./do/IPBlockCounter.js";
export { IPBlockCounter };

// ボットのCIDRリストとUAパターンをグローバルにキャッシュ
let botCidrsCache = null;
let unwantedBotPatternsCache = null;

export default {
  /**
   * すべての受信リクエストを処理します。
   */
  async fetch(request, env, ctx) {
    return handle(request, env, ctx);
  },

  /**
   * Cron Triggerによって定期的に実行され、永続ブロックリストを同期します。
   */
  async scheduled(event, env, ctx) {
    console.log("Cron Trigger fired: Syncing permanent block list...");

    // DOから永続ブロック対象のIPリストを取得
    const id = env.IP_COUNTER.idFromName("sync-job"); // 固定名でDOスタブを取得
    const stub = env.IP_COUNTER.get(id);
    const res = await stub.fetch("https://internal/list-high-count");

    if (!res.ok) {
      console.error(`Failed to fetch high count IPs from DO. Status: ${res.status}`);
      return;
    }

    const ipsToBlock = await res.json();
    if (!ipsToBlock || ipsToBlock.length === 0) {
      console.log("No new IPs to permanently block.");
      return;
    }

    // KVに一括書き込み
    const promises = ipsToBlock.map(ip =>
      env.BOT_BLOCKER_KV.put(ip, "permanent-block")
    );
    await Promise.all(promises);

    console.log(`Synced ${ipsToBlock.length} permanent block IPs to KV.`);
  }
};


// --- 2. メインロジック ---

/**
 * リクエストを処理するコア関数
 */
async function handle(request, env, ctx) {
  const ua = request.headers.get("User-Agent") || "UA_NOT_FOUND";
  const ip = request.headers.get("CF-Connecting-IP") || "IP_NOT_FOUND";
  const { pathname } = new URL(request.url);
  const path = pathname.toLowerCase();

  // --- ステップ 1: KVでIPのブロック状態を最優先で確認 (高速化) ---
  const status = await env.BOT_BLOCKER_KV.get(ip, { cacheTtl: 300 }); // 5分間キャッシュ
  if (status === "permanent-block" || status === "temp-1" || status === "temp-2") {
    console.log(`[KV BLOCK] IP=${ip} status=${status}`);
    return new Response("Not Found", { status: 404 });
  }

  // --- ステップ 2: 静的な即時ブロック (違反カウント対象外) ---
  // 静的IPリストによるブロック
  const staticBlockIps = new Set([
    // "192.0.2.0/24", // 例
  ]);
  for (const block of staticBlockIps) {
    if (ipInCidr(ip, block)) {
      return logAndBlock(ip, ua, "static-ip", env, ctx);
    }
  }
  // 不正なパススキャンによるブロック
  if (path.includes("/wp-") || path.endsWith(".php") || path.includes("/phpmyadmin") ||
      path.endsWith("/.env") || path.endsWith("/config") || path.includes("/admin/") ||
      path.includes("/dbadmin")) {
    return logAndBlock(ip, ua, "path-scan", env, ctx);
  }

  // --- ステップ 3: 処理不要なリクエストのスキップ ---
  const EXT_SKIP = /\.(jpg|jpeg|png|gif|svg|webp|js|css|woff2?|ttf|ico|map|txt|eot|otf|json|xml|avif)(\?|$)/;
  const PATH_SKIP = path.startsWith("/wpm@") || path.includes("/cart.js") ||
                    path.includes("/recommendations/") || path.startsWith("/_t/");
  if (EXT_SKIP.test(path) || PATH_SKIP) {
    return fetch(request);
  }

  // --- ステップ 4: ログ用のUA分類とコンソール出力 ---
  const servicePattern = /(python-requests|aiohttp|monitor|insights)/i;
  const botPattern = /(bot|crawl|spider|slurp|fetch|headless|preview|externalagent|barkrowler|bingbot|petalbot)/i;
  let label;
  if (servicePattern.test(ua)) {
    label = "[S]"; // Service
  } else if (botPattern.test(ua)) {
    label = "[B]"; // Bot
  } else {
    label = "[H]"; // Human
  }
  console.log(`${label} ${request.url} IP=${ip} UA=${ua}`);


  // --- ステップ 5: 振る舞い検知とUAベースのブロック (違反カウント対象) ---
  // A. 望ましくないボットのUAパターンに一致する場合
  if (label === "[B]") {
    if (unwantedBotPatternsCache === null) {
      const patternsJson = await env.LOCALE_FANOUT.get("UNWANTED_BOT_UA_PATTERNS");
      unwantedBotPatternsCache = patternsJson ? JSON.parse(patternsJson) : [];
    }
    for (const patt of unwantedBotPatternsCache) {
      try {
        if (new RegExp(patt, "i").test(ua)) {
          await handleViolation(ip, ua, `unwanted-bot:${patt}`, env, ctx);
          return new Response("Not Found", { status: 404 });
        }
      } catch (e) {
        console.error(`Invalid regex pattern in UNWANTED_BOT_UA_PATTERNS: ${patt}`, e);
      }
    }
  }

  // B. 人間と思われるアクセスの異常な振る舞い (locale-fanout)
  if (label === "[H]") {
    const fanout = await localeFanoutCheck(ip, extractLocale(path), env, ctx);
    if (!fanout.allow) {
      await handleViolation(ip, ua, fanout.reason, env, ctx);
      return new Response("Not Found", { status: 404 });
    }
  }
  
  // C. 正規ボットを偽装している場合
  if (ua.startsWith("AmazonProductDiscovery/1.0")) {
    const isVerified = await verifyBotIp(ip, "amazon", env);
    if (!isVerified) {
      await handleViolation(ip, ua, "amazon-impersonation", env, ctx);
      return new Response("Not Found", { status: 404 });
    }
  }
  // 他のボットの検証もここに追加
  // if (ua.startsWith("ClaudeBot/1.0")) { ... }

  // --- ステップ 6: 全てのチェックを通過 ---
  return fetch(request);
}


// --- 3. コアヘルパー関数 ---

/**
 * 違反を検知した際の統一処理。DOでカウントし、KVとR2を更新する。
 */
async function handleViolation(ip, ua, reason, env, ctx) {
  console.log(`[VIOLATION] IP=${ip} reason=${reason} UA=${ua}`);

  // DOを呼び出して違反回数をインクリメント＆取得
  const id = env.IP_COUNTER.idFromName(ip);
  const stub = env.IP_COUNTER.get(id);
  const res = await stub.fetch(new Request("https://internal/count", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ip }),
  }));

  if (!res.ok) {
    console.error(`DO countViolation failed for IP=${ip}. Status: ${res.status}`);
    return;
  }

  const { count } = await res.json();

  // 違反回数に応じてKVとR2を更新
  if (count === 1) {
    ctx.waitUntil(env.BOT_BLOCKER_KV.put(ip, "temp-1", { expirationTtl: 600 })); // 10分
  } else if (count === 2) {
    ctx.waitUntil(env.BOT_BLOCKER_KV.put(ip, "temp-2", { expirationTtl: 600 })); // 10分
  } else if (count >= 3) {
    // 永久ブロック
    ctx.waitUntil(env.BOT_BLOCKER_KV.put(ip, "permanent-block"));

    // R2に監査ログを永続保存
    const record = JSON.stringify({
      ip,
      userAgent: ua,
      reason,
      count,
      timestamp: new Date().toISOString(),
    });
    // R2のキーが重複しないようにタイムスタンプを追加
    ctx.waitUntil(env.BLOCKLIST_R2.put(`${ip}-${Date.now()}.json`, record));
  }
}

/**
 * 汎用的なIP検証関数。KVのCIDRリストと照合する。
 */
async function verifyBotIp(ip, botKey, env) {
  if (botCidrsCache === null) {
    botCidrsCache = await env.BOT_BLOCKER_KV.get("BOT_CIDRS", { type: "json", cacheTtl: 3600 });
  }
  const cidrs = botCidrsCache ? botCidrsCache[botKey] : null;

  if (!cidrs || !Array.isArray(cidrs) || cidrs.length === 0) {
    console.warn(`CIDR list for bot '${botKey}' is empty or not found in KV.`);
    // CIDRリストが存在しない場合、安全のため偽装と判断
    return false;
  }
  return cidrs.some(cidr => ipInCidr(ip, cidr));
}

/**
 * 違反カウント対象外の、静的なルールに基づく即時ブロック
 */
async function logAndBlock(ip, ua, reason, env, ctx) {
  console.log(`[STATIC BLOCK] IP=${ip} reason=${reason} UA=${ua}`);
  // こちらは違反カウントに加算せず、即座にブロックする
  // 必要であれば、この種のブロックも別のR2ログに記録するなどの処理を追加可能
  return new Response("Not Found", { status: 404 });
}


// --- 4. ユーティリティ関数 ---

/**
 * パスからロケールを抽出する (例: /en/path -> en)
 */
function extractLocale(path) {
  const seg = path.split('/').filter(Boolean)[0];
  if (!seg) return 'root';
  if (/^[a-z]{2}(-[a-z]{2})?$/.test(seg)) return seg;
  return 'root';
}

/**
 * IPアドレスをBigIntに変換する (IPv4/v6対応)
 */
function ipToBigInt(ip) {
  if (ip.includes(':')) { // IPv6
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
    return full.reduce((acc, p) => (acc << 16n) + BigInt(`0x${p || '0'}`), 0n);
  } else { // IPv4
    return ip.split('.').reduce((acc, p) => (acc << 8n) + BigInt(p), 0n);
  }
}

/**
 * IPアドレスがCIDR範囲内かチェックする (IPv4/v6対応)
 */
function ipInCidr(ip, cidr) {
  try {
    const [base, prefixStr] = cidr.split('/');
    const prefix = parseInt(prefixStr, 10);
    const isV6 = cidr.includes(':');
    const totalBits = isV6 ? 128 : 32;

    if (isNaN(prefix) || prefix < 0 || prefix > totalBits) return false;
    
    // IPとCIDRのバージョンが一致するか確認
    if (isV6 !== ip.includes(':')) return false;

    const ipVal = ipToBigInt(ip);
    const baseVal = ipToBigInt(base);
    // BigIntのビットシフトは末尾がnで終わる必要がある
    const mask = ( (1n << BigInt(prefix)) - 1n ) << BigInt(totalBits - prefix);

    return (ipVal & mask) === (baseVal & mask);
  } catch (e) {
    console.error(`[ipInCidr] Error: ip='${ip}' cidr='${cidr}'`, e);
    return false;
  }
}

/**
 * 短時間での複数ロケールへのアクセス(fanout)を検知する
 */
async function localeFanoutCheck(ip, locale, env, ctx) {
  if (!ip) return { allow: true };
  const LOCALE_WINDOW_MS = 30 * 1000; // 30秒
  const LOCALE_THRESHOLD = 3;        // 3ロケール以上
  const now = Date.now();

  let data;
  try {
    const raw = await env.LOCALE_FANOUT.get(ip);
    data = raw ? JSON.parse(raw) : { locales: {} };
  } catch (e) {
    console.error("localeFanoutCheck: KV read/parse error", e);
    data = { locales: {} };
  }

  // 古いエントリを削除
  let needsWrite = false;
  for (const [loc, ts] of Object.entries(data.locales)) {
    if (now - ts > LOCALE_WINDOW_MS) {
      delete data.locales[loc];
      needsWrite = true;
    }
  }

  // 新しいロケールを追加
  if (!data.locales[locale]) {
    data.locales[locale] = now;
    needsWrite = true;
  }

  // 閾値を超えたかチェック
  if (Object.keys(data.locales).length >= LOCALE_THRESHOLD) {
    // 違反が確定したら、このIPの一時的な行動追跡データは削除する
    ctx.waitUntil(env.LOCALE_FANOUT.delete(ip));
    return { allow: false, reason: `locale-fanout:${Object.keys(data.locales).length}` };
  }

  if (needsWrite) {
    // 1分間だけキーを保持する（ウィンドウ期間の倍）
    ctx.waitUntil(env.LOCALE_FANOUT.put(ip, JSON.stringify(data), { expirationTtl: 60 }));
  }

  return { allow: true };
}
