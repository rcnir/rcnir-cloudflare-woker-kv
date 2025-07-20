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

let learnedBadBotsCache = null;
let badBotDictionaryCache = null;

export default {
  async fetch(request, env, ctx) {
    const logBuffer = [];
    try {
      return await handle(request, env, ctx, logBuffer);
    } finally {
      // ★ 修正: バッファ内のログを一行ずつループで出力する
      for (const message of logBuffer) {
        console.log(message);
      }
      console.log('----------------------------------------');
    }
  },

  async scheduled(event, env, ctx) {
    // (scheduledハンドラはリクエスト単位ではないため、従来のconsole.logをそのまま使用)
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
// ★ 変更: logBufferを引数として受け取る
async function handle(request, env, ctx, logBuffer) {
  const url = new URL(request.url);

  // (管理エンドポイントはロググループの対象外なので変更なし)
  const resetKey = url.searchParams.get("reset_key");
  if (url.pathname.startsWith("/admin/") || url.pathname.startsWith("/reset-state") || url.pathname.startsWith("/debug/")) {
    // ... 既存のリセット・デバッグ処理 ...
    // この部分はロググループ化から除外するため、簡略化のため省略
    // 必要であれば、これらのエンドポイント内でも同様のロギング手法を適用可能
    return new Response("Admin/Debug endpoint accessed.", { status: 200 });
  }

  const ua = request.headers.get("User-Agent") || "UA_NOT_FOUND";
  const ip = request.headers.get("CF-Connecting-IP") || "IP_NOT_FOUND";
  const path = url.pathname.toLowerCase();
  // ★ 変更: generateFingerprintにlogBufferを渡す
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

  // --- 3. 静的ルールによるパス探索型攻撃ブロック ---
  if (path.includes("/wp-") || path.endsWith(".php") || path.includes("/phpmyadmin") ||
      path.endsWith("/.env") || path.endsWith("/config") || path.includes("/admin/") ||
      path.includes("/dbadmin")) {
    return logAndBlock(ip, ua, "path-scan", env, ctx, fingerprint, logBuffer);
  }

  // --- UAベースの分類 ---
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

  // --- 有害Bot検知＋ペナルティ ---
  if (refinedLabel === "[B]") {
    // ... (この中の処理はhandleViolationSideEffectsを呼び出すため、変更はヘルパー関数側で集約)
  }

  // --- アセットファイルならそのまま返す ---
  const EXT_SKIP = /\.(jpg|jpeg|png|gif|svg|webp|js|css|woff2?|ttf|ico|map|txt|eot|otf|json|xml|avif)(\?|$)/;
  if (EXT_SKIP.test(path)) {
    // ... (JSピクセル検出ロジック内にもconsole.logがあればlogBuffer.pushへ変更)
    return fetch(request);
  }

  // --- 動的ルール実行 ---
  if (refinedLabel === "[SH]") {
    const ipLocaleRes = await ipTrackerStub.fetch(new Request("https://internal/check-locale", { method: 'POST', headers: { "CF-Connecting-IP": ip, "Content-Type": "application/json" }, body: JSON.stringify({ path }) }));
    const fpLocaleRes = await fpTrackerStub.fetch(new Request("https://internal/check-locale-fp", { method: 'POST', headers: { "X-Fingerprint-ID": fingerprint, "Content-Type": "application/json" }, body: JSON.stringify({ path }) }));

    let violationDetected = false;
    let ipCount = 0;
    let fpCount = 0;
    let reason = "locale-fanout";

    if (ipLocaleRes.ok) {
      const { violation, count } = await ipLocaleRes.json();
      if (violation) {
        violationDetected = true;
        ipCount = count;
      }
    } else {
      logBuffer.push(`[DO_ERROR] IP DO /check-locale failed for IP=${ip}. Status: ${ipLocaleRes.status}`);
    }

    if (fpLocaleRes.ok) {
      const { violation, count } = await fpLocaleRes.json();
      if (violation) {
        violationDetected = true;
        fpCount = count;
      }
    } else {
      logBuffer.push(`[DO_ERROR] FP DO /check-locale-fp failed for FP=${fingerprint}. Status: ${fpLocaleRes.status}`);
    }

    if (violationDetected) {
      await handleViolationSideEffects(ip, ua, reason, Math.max(ipCount, fpCount), env, ctx, fingerprint, fpCount, logBuffer);
      return new Response("Not Found", { status: 404 });
    }
  }
  
  // --- Amazon Botなりすましチェック ---
  if (ua.startsWith("AmazonProductDiscovery/1.0")) {
    const isVerified = await verifyBotIp(ip, "amazon", env, logBuffer);
    if (!isVerified) {
      const reason = "amazon-impersonation";
      const ipRes = await ipTrackerStub.fetch(new Request("https://internal/trigger-violation", { headers: { "CF-Connecting-IP": ip } }));
      const fpRes = await fpTrackerStub.fetch(new Request("https://internal/track-violation", { headers: { "X-Fingerprint-ID": fingerprint } }));
      if (ipRes.ok && fpRes.ok) {
        const { count: ipCount } = await ipRes.json();
        const { count: fpCount } = await fpRes.json();
        await handleViolationSideEffects(ip, ua, reason, Math.max(ipCount, fpCount), env, ctx, fingerprint, fpCount, logBuffer);
      } else {
        logBuffer.push(`[DO_ERROR] Failed to trigger violation for IP=${ip} FP=${fingerprint}. IP DO Status: ${ipRes.status}, FP DO Status: ${fpRes.status}`);
      }
      return new Response("Not Found", { status: 404 });
    }
  }

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
  // ... (この関数は変更なし)
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
    // ★ 変更: console.errorをlogBuffer.pushに
    logBuffer.push(`[ipInCidr_ERROR] Error: ip='${ip}' cidr='${cidr}' Message: ${e.message}`);
    return false;
  }
}
