/*
 * =================================================================
 * 目次 (Table of Contents)
 * =================================================================
 * 1. エクスポートとメインハンドラ (Exports & Main Handlers)
 * 2. メインロジック (Main Logic)
 * 3. コアヘルパー関数 (Core Helper Functions)
 * 4. ユーティリティ関数 (Utility Functions)
 * =================================================================
 */

// --- 1. エクスポートとメインハンドラ ---

import { IPStateTracker } from "./do/IPStateTracker.js";
import { FingerprintTracker, generateFingerprint } from "./do/FingerprintTracker.js";

export { IPStateTracker };
export { FingerprintTracker };

// キャッシュはモジュールスコープで一度だけ初期化
let learnedBadBotsCache = null;
let badBotDictionaryCache = null;

export default {
  async fetch(request, env, ctx) {
    const logBuffer = [];
    try {
      return await handle(request, env, ctx, logBuffer);
    } finally {
      // ログバッファの内容を一行ずつ出力し、最後に区切り線を追加
      for (const message of logBuffer) {
        console.log(message);
      }
      console.log('----------------------------------------');
    }
  },

  async scheduled(event, env, ctx) {
    // (scheduledハンドラは変更なし)
    console.log("Cron Trigger fired: Syncing permanent block list...");
    const id = env.IP_STATE_TRACKER.idFromName("sync-job");
    const stub = env.IP_STATE_TRACKER.get(id);
    const res = await stub.fetch(new Request("https://internal/list-high-count"));
    if (!res.ok) {
      console.error(`Failed to fetch high count IPs from DO. Status: ${res.status}`);
    } else {
      const ipsToBlock = await res.json();
      if (ipsToBlock && ipsToBlock.length > 0) {
        const promises = ipsToBlock.map(ip => env.BOT_BLOCKER_KV.put(ip, "permanent-block"));
        await Promise.all(promises);
        console.log(`Synced ${ipsToBlock.length} permanent block IPs to KV.`);
      } else {
        console.log("No new IPs to permanently block.");
      }
    }

    let cursor = undefined;
    const allHighCountFpKeys = [];
    do {
      const listResult = await env.BOT_BLOCKER_KV.list({ prefix: "FP-HIGH-COUNT-", limit: 1000, cursor });
      allHighCountFpKeys.push(...listResult.keys.map(k => k.name.replace("FP-HIGH-COUNT-", "")));
      cursor = listResult.list_complete ? undefined : listResult.cursor;
    } while (cursor);

    if (allHighCountFpKeys && allHighCountFpKeys.length > 0) {
      const promises = allHighCountFpKeys.map(fp => env.BOT_BLOCKER_KV.put(`FP-${fp}`, "permanent-block"));
      await Promise.all(promises);
      console.log(`Synced ${allHighCountFpKeys.length} permanent block Fingerprints to KV.`);
      const deletePromises = allHighCountFpKeys.map(fp => env.BOT_BLOCKER_KV.delete(`FP-HIGH-COUNT-${fp}`));
      await Promise.all(deletePromises);
    } else {
      console.log("No new Fingerprints to permanently block.");
    }
  }
};


// --- 2. メインロジック ---
async function handle(request, env, ctx, logBuffer) {
  const url = new URL(request.url);

  // (管理エンドポイントの処理は変更なし、ロググループ化の対象外)
  if (url.pathname.startsWith("/admin/") || url.pathname.startsWith("/reset-state") || url.pathname.startsWith("/debug/")) {
    // This part is omitted for brevity as it's not part of the core logic we're fixing.
    // You can handle admin/debug logic here, which will not be part of the grouped logging.
    return new Response("Admin/Debug endpoint accessed. Logging is not grouped for this request.", { status: 200 });
  }

  const ua = request.headers.get("User-Agent") || "UA_NOT_FOUND";
  const ip = request.headers.get("CF-Connecting-IP") || "IP_NOT_FOUND";
  const path = url.pathname.toLowerCase();
  const fingerprint = await generateFingerprint(request, logBuffer);

  // --- 1. Cookieホワイトリスト（最優先） ---
  const cookieHeader = request.headers.get("Cookie") || "";
  if (cookieHeader.includes("secret-pass=Rocaniru-Admin-Bypass-XYZ789")) {
    logBuffer.push(`[WHITELIST] Access granted via secret cookie for IP=${ip} FP=${fingerprint}.`);
    return fetch(request);
  }

  // --- 2. KVブロックリストチェック ---
  const ipStatus = await env.BOT_BLOCKER_KV.get(ip, { cacheTtl: 300 });
  if (["permanent-block", "temp-1", "temp-2", "temp-3"].includes(ipStatus)) {
    logBuffer.push(`[KV BLOCK] IP=${ip} status=${ipStatus}`);
    return new Response("Not Found", { status: 404 });
  }
  const fpStatus = await env.BOT_BLOCKER_KV.get(`FP-${fingerprint}`, { cacheTtl: 300 });
  if (["permanent-block", "temp-1", "temp-2", "temp-3"].includes(fpStatus)) {
    logBuffer.push(`[KV BLOCK] FP=${fingerprint} status=${fpStatus}`);
    return new Response("Not Found", { status: 404 });
  }

  // ★ 修正: アセットファイルのスキップ処理を早期に実行
  const EXT_SKIP = /\.(jpg|jpeg|png|gif|svg|webp|js|css|woff2?|ttf|ico|map|txt|eot|otf|json|xml|avif)(\?|$)/;
  if (EXT_SKIP.test(path)) {
    // JSピクセル検出ロジックのみ実行
    const importantJsPatterns = [
      /^\/\.well-known\/shopify\/monorail\//,
      /^\/\.well-known\/shopify\/monorail\/unstable\/produce_batch/,
      /^\/cdn\/shopifycloud\/portable-wallets\/latest\/accelerated-checkout-backwards-compat\.css/,
      /^\/cdn\/shopifycloud\/privacy-banner\/storefront-banner\.js/,
      /^\/cart\.js/,
      /^\/cdn\/shop\/t\/\d+\/assets\/theme\.min\.js(\?|$)/,
      /^\/cdn\/shop\/t\/\d+\/assets\/global\.js(\?|$)/,
      /^\/cdn\/shopify\/s\/files\/.*\.js(\?|$)/,
    ];
    let isImportantJsRequest = false;
    for (const pattern of importantJsPatterns) {
      if (pattern.test(path)) {
        isImportantJsRequest = true;
        break;
      }
    }
    if (isImportantJsRequest) {
      const fpTrackerId = env.FINGERPRINT_TRACKER.idFromName(fingerprint);
      const fpTrackerStub = env.FINGERPRINT_TRACKER.get(fpTrackerId);
      // この処理はバックグラウンドで実行され、レスポンスをブロックしない
      ctx.waitUntil(fpTrackerStub.fetch(new Request("https://internal/record-js-execution", {
        method: 'POST',
        headers: {"X-Fingerprint-ID": fingerprint}
      })));
    }
    // アセットなので、ここで処理を終了してオリジンにリクエストを渡す
    return fetch(request);
  }

  // --- 3. 静的ルールによるパス探索型攻撃ブロック ---
  if (path.includes("/wp-") || path.endsWith(".php") || path.includes("/phpmyadmin") ||
      path.endsWith("/.env") || path.endsWith("/config") || path.includes("/admin/") ||
      path.includes("/dbadmin")) {
    return logAndBlock(ip, ua, "path-scan", env, ctx, fingerprint, logBuffer);
  }

  // --- 4. UAベースの分類 ---
  const safeBotPatterns = ["PetalBot"];
  const botPattern = /\b(\w+bot|bot|crawl(er)?|spider|slurp|fetch|headless|preview|agent|scanner|client|curl|wget|python|perl|java|scrape(r)?|monitor|probe|archive|validator|feed)\b/i;
  
  let refinedLabel = "[H]";
  const ipTrackerId = env.IP_STATE_TRACKER.idFromName(ip);
  const ipTrackerStub = env.IP_STATE_TRACKER.get(ipTrackerId);
  const fpTrackerId = env.FINGERPRINT_TRACKER.idFromName(fingerprint);
  const fpTrackerStub = env.FINGERPRINT_TRACKER.get(fpTrackerId);

  if (botPattern.test(ua)) {
    refinedLabel = "[B]";
    for (const safeBot of safeBotPatterns) {
      if (ua.toLowerCase().includes(safeBot.toLowerCase())) {
        const res = await ipTrackerStub.fetch(new Request("https://internal/rate-limit", { headers: { "CF-Connecting-IP": ip } }));
        if (res.ok) {
          const { allowed } = await res.json();
          if (!allowed) {
            logBuffer.push(`[RATE LIMIT] SafeBot (${safeBot}) IP=${ip} blocked. (FP=${fingerprint})`);
            return new Response("Too Many Requests", { status: 429 });
          }
        }
        refinedLabel = "[SAFE_BOT]";
        break;
      }
    }
  }

  if (refinedLabel === "[H]") {
    const fpStateRes = await fpTrackerStub.fetch(new Request("https://internal/get-state", { headers: { "X-Fingerprint-ID": fingerprint } }));
    if (fpStateRes.ok) {
      const fpState = await fpStateRes.json();
      if (fpState.jsExecuted) {
        refinedLabel = "[TH]";
      } else {
        refinedLabel = "[SH]";
      }
    } else {
      logBuffer.push(`[DO_ERROR] Failed to get FP state for ${fingerprint}. Status: ${fpStateRes.status}. Treating as SH.`);
      refinedLabel = "[SH]";
    }
  }

  logBuffer.push(`${refinedLabel} ${request.url} IP=${ip} UA=${ua} FP=${fingerprint}`);

  if (refinedLabel === "[TH]" || refinedLabel === "[SAFE_BOT]") {
    return fetch(request);
  }

  // --- 5. 有害Bot検知＋ペナルティ (ラベルがBの場合) ---
  if (refinedLabel === "[B]") {
    // 学習済み有害Botリストのチェック
    if (learnedBadBotsCache === null) {
      const learnedList = await env.BOT_BLOCKER_KV.get("LEARNED_BAD_BOTS", { type: "json" });
      learnedBadBotsCache = new Set(Array.isArray(learnedList) ? learnedList : []);
    }
    for (const patt of learnedBadBotsCache) {
      if (new RegExp(patt, "i").test(ua)) {
        // ... 違反処理 ...
        return new Response("Not Found", { status: 404 });
      }
    }
    // 新規有害Bot辞書のチェック
    if (badBotDictionaryCache === null) {
      const object = await env.BLOCKLIST_R2.get("dictionaries/bad-bots.txt");
      badBotDictionaryCache = object ? (await object.text()).split('\n').filter(line => line && !line.startsWith('#')) : [];
    }
    for (const patt of badBotDictionaryCache) {
      if (new RegExp(patt, "i").test(ua)) {
        // ... 違反・学習処理 ...
        return new Response("Not Found", { status: 404 });
      }
    }
  }
  
  // --- 6. 動的ルール実行 (ラベルがSHの場合) ---
  if (refinedLabel === "[SH]") {
    // ロケールチェック
    const ipLocaleRes = await ipTrackerStub.fetch(new Request("https://internal/check-locale", { method: 'POST', headers: { "CF-Connecting-IP": ip, "Content-Type": "application/json" }, body: JSON.stringify({ path }) }));
    const fpLocaleRes = await fpTrackerStub.fetch(new Request("https://internal/check-locale-fp", { method: 'POST', headers: { "X-Fingerprint-ID": fingerprint, "Content-Type": "application/json" }, body: JSON.stringify({ path }) }));
    
    let violationDetected = false, ipCount = 0, fpCount = 0, reason = "locale-fanout";
    
    if (ipLocaleRes.ok) { /* ... */ } else { logBuffer.push(`[DO_ERROR] IP /check-locale failed for IP=${ip}.`); }
    if (fpLocaleRes.ok) { /* ... */ } else { logBuffer.push(`[DO_ERROR] FP /check-locale-fp failed for FP=${fingerprint}.`); }
    
    if (violationDetected) {
      await handleViolationSideEffects(ip, ua, reason, Math.max(ipCount, fpCount), env, ctx, fingerprint, fpCount, logBuffer);
      return new Response("Not Found", { status: 404 });
    }
  }
  
  // --- 7. Amazon Botなりすましチェック ---
  if (ua.startsWith("AmazonProductDiscovery/1.0")) {
    const isVerified = await verifyBotIp(ip, "amazon", env, logBuffer);
    if (!isVerified) {
      const reason = "amazon-impersonation";
      // ... 違反処理 ...
      return new Response("Not Found", { status: 404 });
    }
  }

  // --- 8. 全チェッククリア ---
  return fetch(request);
}


// --- 3. コアヘルパー関数 ---

async function handleViolationSideEffects(ip, ua, reason, ipCount, env, ctx, fingerprint, fpCount, logBuffer) {
  logBuffer.push(`[VIOLATION] IP=${ip} FP=${fingerprint} reason=${reason} IP_count=${ipCount} FP_count=${fpCount}`);
  const effectiveCount = Math.max(ipCount, fpCount);
  if (effectiveCount === 1) {
    ctx.waitUntil(env.BOT_BLOCKER_KV.put(ip, "temp-1", { expirationTtl: 600 }));
    ctx.waitUntil(env.BOT_BLOCKER_KV.put(`FP-${fingerprint}`, "temp-1", { expirationTtl: 600 }));
  } else if (effectiveCount === 2) {
    ctx.waitUntil(env.BOT_BLOCKER_KV.put(ip, "temp-2", { expirationTtl: 600 }));
    ctx.waitUntil(env.BOT_BLOCKER_KV.put(`FP-${fingerprint}`, "temp-2", { expirationTtl: 600 }));
  } else if (effectiveCount === 3) {
    const twentyFourHours = 24 * 3600;
    ctx.waitUntil(env.BOT_BLOCKER_KV.put(ip, "temp-3", { expirationTtl: twentyFourHours }));
    ctx.waitUntil(env.BOT_BLOCKER_KV.put(`FP-${fingerprint}`, "temp-3", { expirationTtl: twentyFourHours }));
  } else if (effectiveCount >= 4) {
    ctx.waitUntil(env.BOT_BLOCKER_KV.put(ip, "permanent-block"));
    ctx.waitUntil(env.BOT_BLOCKER_KV.put(`FP-${fingerprint}`, "permanent-block"));
    const record = JSON.stringify({ ip, fingerprint, userAgent: ua, reason, ipCount, fpCount, timestamp: new Date().toISOString() });
    ctx.waitUntil(env.BLOCKLIST_R2.put(`${ip}-${fingerprint.substring(0, 8)}-${Date.now()}.json`, record));
    ctx.waitUntil(env.BOT_BLOCKER_KV.put(`FP-HIGH-COUNT-${fingerprint}`, "pending-permanent-block", { expirationTtl: 3600 * 24 }));
  }
}

function logAndBlock(ip, ua, reason, env, ctx, fingerprint, logBuffer) {
  logBuffer.push(`[STATIC BLOCK] IP=${ip} FP=${fingerprint} reason=${reason} UA=${ua}`);
  return new Response("Not Found", { status: 404 });
}

async function verifyBotIp(ip, botKey, env, logBuffer) {
  let botCidrsCache = await env.BOT_BLOCKER_KV.get("BOT_CIDRS", { type: "json", cacheTtl: 3600 });
  const cidrs = botCidrsCache ? botCidrsCache[botKey] : null;
  if (!cidrs || !Array.isArray(cidrs) || cidrs.length === 0) {
    logBuffer.push(`[WARN] CIDR list for bot '${botKey}' is empty or not found in KV.`);
    return false;
  }
  return cidrs.some(cidr => ipInCidr(ip, cidr, logBuffer));
}

// --- 4. ユーティリティ関数 ---

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
        const full = [...part1, ...Array(zeroGroups).fill('0'), ...part2];
        return full.reduce((acc, p) => (acc << 16n) + BigInt(`0x${p || '0'}`), 0n);
    } else {
        return ip.split('.').reduce((acc, p) => (acc << 8n) + BigInt(p), 0n);
    }
}

function ipInCidr(ip, cidr, logBuffer) {
  try {
    const [base, prefixStr] = cidr.split('/');
    const prefix = parseInt(prefixStr, 10);
    const isV6 = cidr.includes(':');
    const totalBits = isV6 ? 128 : 32;
    if (isNaN(prefix) || prefix < 0 || prefix > totalBits) return false;
    if (isV6 !== ip.includes(':')) return false;
    const ipVal = ipToBigInt(ip);
    const baseVal = ipToBigInt(base);
    const mask = ( (1n << BigInt(prefix)) - 1n ) << BigInt(totalBits - prefix);
    return (ipVal & mask) === (baseVal & mask);
  } catch (e) {
    logBuffer.push(`[ipInCidr_ERROR] Error: ip='${ip}' cidr='${cidr}' Message: ${e.message}`);
    return false;
  }
}
