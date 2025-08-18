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

/*
 * =================================================================
 * 便利なターミナルコマンド (Useful Terminal Commands)
 * =================================================================
 *
 * --- ログ監視 (Log Monitoring) ---
 *
 * ■ 全てのログを表示
 * npx wrangler tail shopify-bot-blocker
 *
 * ■ TH判定 (信頼された人間) のみ表示
 * npx wrangler tail shopify-bot-blocker | grep -F "[TH]"
 *
 * ■ SH判定 (不審な人間) のみ表示
 * npx wrangler tail shopify-bot-blocker | grep -F "[SH]"
 *
 * ■ B判定 (ボット) のみ表示
 * npx wrangler tail shopify-bot-blocker | grep -F "[B]"
 *
 * ■ VIOLATION (違反検知) のみ表示
 * npx wrangler tail shopify-bot-blocker | grep "\[VIOLATION\]"
 *
 * --- KVストア管理 (KV Store Management) ---
 *
 * ■ ブロック中の全IP/FPを一覧表示
 * npx wrangler kv key list --namespace-id="7da99382fc3945bd87bc65f55c9ea1fb"
 *
 * ■ 特定のIP/FPのブロック状態を確認 (例: "192.0.2.1")
 * npx wrangler kv key get --namespace-id="7da99382fc3945bd87bc65f55c9ea1fb" "ここにIPアドレスかFPキーを入力"
 *
 * --- R2バケット管理 (R2 Bucket Management) ---
 *
 * ■ 永続ブロックされたボットの全ログファイル一覧を表示
 * npx wrangler r2 object list rocaniiru-log
 *
 * ■ 特定のログファイルの中身を表示 (例: "192.0.2.1-a1b2c3d4-...")
 * npx wrangler r2 object get rocaniiru-log "ここにファイル名を入力"
 *
 * --- デプロイ (Deployment) ---
 *
 * ■ WorkerをCloudflareにデプロイ
 * npx wrangler deploy
 *
 * =================================================================
 */

// --- 1. エクスポートとメインハンドラ ---

import { IPStateTracker } from "./do/IPStateTracker.js";
import { FingerprintTracker, generateFingerprint } from "./do/FingerprintTracker.js";

export { IPStateTracker };
export { FingerprintTracker };

// キャッシュはモジュールスコープで一度だけ初期化
let workerConfigCache = null;
let asnBlocklistCache = null;
let activeBadBotListCache = null;

export default {
  async fetch(request, env, ctx) {
    const logBuffer = [];
    try {
      return await handle(request, env, ctx, logBuffer);
    } finally {
      for (const message of logBuffer) {
        console.log(message);
      }
      console.log('----------------------------------------');
    }
  },

  async scheduled(event, env, ctx) {
    console.log("Cron Trigger fired: Syncing lists...");

    const object = await env.BLOCKLIST_R2.get("dictionaries/bad-bots.txt");
    if (object) {
      const text = await object.text();
      const list = text.split('\n').filter(line => line && !line.startsWith('#'));
      await env.BOT_BLOCKER_KV.put("SYSTEM_BAD_BOT_LIST", JSON.stringify(list));
      console.log(`Synced ${list.length} bad bot patterns from R2 to KV.`);
    } else {
      console.error("Failed to get bad-bots.txt from R2.");
    }
    
    console.log("Syncing permanent block list...");
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

  if (url.pathname.startsWith("/admin/") || url.pathname.startsWith("/reset-state") || url.pathname.startsWith("/debug/")) {
    return new Response(`Admin/Debug endpoint accessed: ${url.pathname}`, { status: 200 });
  }

  // --- スコアリングシステム初期化 ---
  if (workerConfigCache === null) {
    const configJson = await env.BOT_BLOCKER_KV.get("WORKER_CONFIG");
    workerConfigCache = configJson ? JSON.parse(configJson) : {};
    logBuffer.push('[CONFIG] Loaded worker configuration from KV.');
  }
  const config = workerConfigCache;
  let score = 0;
  const signals = [];

  const ua = request.headers.get("User-Agent") || "UA_NOT_FOUND";
  const ip = request.headers.get("CF-Connecting-IP") || "IP_NOT_FOUND";
  const path = url.pathname.toLowerCase();
  const fingerprint = await generateFingerprint(request, logBuffer);

  // --- 1. 絶対的ブロック/許可ルール (スコアリング対象外) ---
  const cookieHeader = request.headers.get("Cookie") || "";
  if (cookieHeader.includes("secret-pass=Rocaniru-Admin-Bypass-XYZ789")) {
    logBuffer.push(`[WHITELIST] Bypassed scoring. IP=${ip}`);
    return fetch(request);
  }

  const [ipStatus, fpStatus] = await Promise.all([
    env.BOT_BLOCKER_KV.get(ip, { cacheTtl: 300 }),
    env.BOT_BLOCKER_KV.get(`FP-${fingerprint}`, { cacheTtl: 300 })
  ]);
  if (ipStatus === "permanent-block" || fpStatus === "permanent-block") {
    logBuffer.push(`[PERMA-BLOCK] Blocked by permanent KV entry. IP=${ip}, FP=${fingerprint}`);
    return new Response("Not Found", { status: 404 });
  }

  if (activeBadBotListCache === null) {
    const listJson = await env.BOT_BLOCKER_KV.get("ACTIVE_BAD_BOT_LIST");
    activeBadBotListCache = new Set(listJson ? JSON.parse(listJson) : []);
  }
  for (const patt of activeBadBotListCache) {
    const flexiblePatt = patt.replace(/^\^|\$$/g, '');
    if (new RegExp(flexiblePatt, "i").test(ua)) {
      logBuffer.push(`[ACTIVE BAD BOT BLOCK] UA matched active list rule: ${patt}`);
      return new Response("Forbidden", { status: 403 });
    }
  }

  const EXT_SKIP = /\.(jpg|jpeg|png|gif|svg|webp|js|css|woff2?|ttf|ico|map|txt|eot|otf|json|xml|avif)(\?|$)/;
  if (EXT_SKIP.test(path)) {
    const importantJsPatterns = [
      /^\/\.well-known\/shopify\/monorail\//, /^\/\.well-known\/shopify\/monorail\/unstable\/produce_batch/,
      /^\/cdn\/shopifycloud\/privacy-banner\/storefront-banner\.js/, /^\/cart\.js/,
      /^\/cdn\/shop\/t\/\d+\/assets\/theme\.min\.js(\?|$)/,
    ];
    if (importantJsPatterns.some(pattern => pattern.test(path))) {
      const fpTrackerId = env.FINGERPRINT_TRACKER.idFromName(fingerprint);
      const fpTrackerStub = env.FINGERPRINT_TRACKER.get(fpTrackerId);
      ctx.waitUntil(fpTrackerStub.fetch(new Request("https://internal/record-js-execution", {
        method: 'POST', headers: {"X-Fingerprint-ID": fingerprint}
      })));
    }
    return fetch(request);
  }

  // --- 2. スコアリング開始 ---
  const ipTrackerId = env.IP_STATE_TRACKER.idFromName(ip);
  const ipTrackerStub = env.IP_STATE_TRACKER.get(ipTrackerId);
  const fpTrackerId = env.FINGERPRINT_TRACKER.idFromName(fingerprint);
  const fpTrackerStub = env.FINGERPRINT_TRACKER.get(fpTrackerId);

  const fpStateRes = await fpTrackerStub.fetch(new Request("https://internal/get-state"));
  let jsExecuted = false;
  if (fpStateRes.ok) {
    jsExecuted = (await fpStateRes.json()).jsExecuted;
  }
  
  if (jsExecuted) {
    logBuffer.push(`[TH] Trusted Human detected. Skipping scoring. IP=${ip}`);
    return fetch(request);
  } else {
    score += config.scores?.jsNotExecuted || 0;
    signals.push("js_not_executed");
  }

  const asn = request.cf?.asn;
  if (asn) {
    if (asnBlocklistCache === null) {
      const blocklistJson = await env.BOT_BLOCKER_KV.get("ASN_BLOCKLIST");
      asnBlocklistCache = blocklistJson ? JSON.parse(blocklistJson) : [];
    }
    if (asnBlocklistCache.includes(String(asn))) {
      score += config.scores?.suspiciousAsn || 0;
      signals.push(`suspicious_asn:${asn}`);
    }
  }

  const staticBlockPatterns = ["/wp-", ".php", "phpinfo", "phpmyadmin", "/.env", "/config", "/admin/", "/dbadmin", "/_profiler", ".aws", "credentials"];
  if (staticBlockPatterns.some(patt => path.includes(patt))) {
    score += config.scores?.staticAttackPattern || 0;
    signals.push(`static_scan:${path}`);
  }

  const secChUa = request.headers.get('Sec-Ch-Ua');
  const acceptLanguage = request.headers.get('Accept-Language');
  if (!secChUa && !acceptLanguage) {
    score += config.scores?.missingHeadersFull || 0;
    signals.push("missing_headers_full");
  } else if (!secChUa || !acceptLanguage) {
    score += config.scores?.missingHeadersPartial || 0;
    signals.push("missing_headers_partial");
  }

  const [ipLocaleRes, fpLocaleRes] = await Promise.all([
    ipTrackerStub.fetch(new Request("https://internal/check-locale", { method: 'POST', headers: { "CF-Connecting-IP": ip, "Content-Type": "application/json" }, body: JSON.stringify({ path }) })),
    fpTrackerStub.fetch(new Request("https://internal/check-locale-fp", { method: 'POST', headers: { "X-Fingerprint-ID": fingerprint, "Content-Type": "application/json" }, body: JSON.stringify({ path }) }))
  ]);
  if ((ipLocaleRes.ok && (await ipLocaleRes.json()).violation) || (fpLocaleRes.ok && (await fpLocaleRes.json()).violation)) {
      score += config.scores?.localeFanout || 0;
      signals.push("locale_fanout");
  }

  if (ua.startsWith("AmazonProductDiscovery/1.0")) {
    const isVerified = await verifyBotIp(ip, "amazon", env, logBuffer);
    if (!isVerified) {
      score += config.scores?.crawlerImpersonation || 0;
      signals.push("amazon_impersonation");
    }
  }
  
  // --- 3. 最終処理 ---
  logBuffer.push(`[SCORE] Final Score: ${score} | Signals: [${signals.join(', ')}] | IP=${ip} | FP=${fingerprint}`);
  logBuffer.push(`[UA] ${ua}`);

  // フェーズ1では、スコアに基づいたアクションはまだ行わない
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
    ctx.waitUntil(env.BOT_BLOCKER_KV.put(ip, "temp-2", { expirationTtl: 1800 }));
    ctx.waitUntil(env.BOT_BLOCKER_KV.put(`FP-${fingerprint}`, "temp-2", { expirationTtl: 1800 }));
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
  ctx.waitUntil(handleViolationSideEffects(ip, ua, reason, 1, env, ctx, fingerprint, 1, logBuffer));
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
