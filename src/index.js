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
let learnedBadBotsCache = null;
let badBotDictionaryCache = null;
let activeBadBotListCache = null;
let asnBlocklistCache = null;

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

  if (url.pathname === '/cf-turnstile/verify') {
    return await handleTurnstileVerification(request, env);
  }

  if (isAdminPath(url.pathname)) {
    if (!isAuthorizedAdmin(request, env)) {
        return new Response("Not Found", { status: 404 });
    }
    return new Response("Admin action completed.", { status: 200 });
  }

  const ua = request.headers.get("User-Agent") || "UA_NOT_FOUND";
  const ip = request.headers.get("CF-Connecting-IP") || "IP_NOT_FOUND";
  const path = url.pathname.toLowerCase();
  const fingerprint = await generateFingerprint(request, logBuffer);

  const cookieHeader = request.headers.get("Cookie") || "";
  if (cookieHeader.includes("secret-pass=Rocaniru-Admin-Bypass-XYZ789")) {
    logBuffer.push(`[WHITELIST] Access granted via secret cookie for IP=${ip}`);
    return fetch(request);
  }

  const [ipStatus, fpStatus] = await Promise.all([
    env.BOT_BLOCKER_KV.get(ip, { cacheTtl: 300 }),
    env.BOT_BLOCKER_KV.get(`FP-${fingerprint}`, { cacheTtl: 300 })
  ]);
  if (["permanent-block", "temp-1", "temp-2", "temp-3"].includes(ipStatus)) {
    logBuffer.push(`[KV BLOCK] IP=${ip} status=${ipStatus}`);
    return new Response("Not Found", { status: 404 });
  }
  if (["permanent-block", "temp-1", "temp-2", "temp-3"].includes(fpStatus)) {
    logBuffer.push(`[KV BLOCK] FP=${fingerprint} status=${fpStatus}`);
    return new Response("Not Found", { status: 404 });
  }

  const asn = request.cf?.asn;
  if (asn) {
    if (asnBlocklistCache === null) {
      const blocklistJson = await env.BOT_BLOCKER_KV.get("ASN_BLOCKLIST");
      asnBlocklistCache = blocklistJson ? JSON.parse(blocklistJson) : [];
    }
    // ★改善点4: ASN即403をTurnstileチャレンジに降格
    if (asnBlocklistCache.includes(String(asn))) {
      logBuffer.push(`[ASN SUSPECT] ASN=${asn} → Turnstile`);
      return presentTurnstileChallenge(request, env);
    }
  }
  
  if (activeBadBotListCache === null) {
    const listJson = await env.BOT_BLOCKER_KV.get("ACTIVE_BAD_BOT_LIST");
    activeBadBotListCache = new Set(listJson ? JSON.parse(listJson) : []);
  }
  for (const patt of activeBadBotListCache) {
    if (new RegExp(patt, "i").test(ua)) {
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

  const staticBlockPatterns = ["/wp-", ".php", "phpinfo", "phpmyadmin", "/.env", "/config", "/admin/", "/dbadmin", "/_profiler", ".aws", "credentials"];
  if (staticBlockPatterns.some(patt => path.includes(patt))) {
    return logAndBlock(ip, ua, "path-scan", env, ctx, fingerprint, logBuffer);
  }

  const safeBotPatterns = ["PetalBot"];
  const botPattern = /\b(\w+bot|bot|crawl(er)?|spider|slurp|fetch|headless|preview|agent|scanner|client|curl|wget|python|perl|java|scrape(r)?|monitor|probe|archive|validator|feed)\b/i;
  
  let refinedLabel = "[H]";
  const ipTrackerId = env.IP_STATE_TRACKER.idFromName(ip);
  const ipTrackerStub = env.IP_STATE_TRACKER.get(ipTrackerId);
  const fpTrackerId = env.FINGERPRINT_TRACKER.idFromName(fingerprint);
  const fpTrackerStub = env.FINGERPRINT_TRACKER.get(fpTrackerId);

  if (botPattern.test(ua)) {
    refinedLabel = "[B]";
    if (safeBotPatterns.some(safeBot => ua.toLowerCase().includes(safeBot.toLowerCase()))) {
      const res = await ipTrackerStub.fetch(new Request("https://internal/rate-limit", { headers: { "CF-Connecting-IP": ip } }));
      if (res.ok) {
        const { allowed } = await res.json();
        if (!allowed) {
          logBuffer.push(`[RATE LIMIT] SafeBot (${safeBotPatterns.find(s => ua.toLowerCase().includes(s.toLowerCase()))}) IP=${ip} blocked.`);
          return new Response("Too Many Requests", { status: 429 });
        }
      }
      refinedLabel = "[SAFE_BOT]";
    }
  }

  if (refinedLabel === "[H]") {
    const fpStateRes = await fpTrackerStub.fetch(new Request("https://internal/get-state", { headers: { "X-Fingerprint-ID": fingerprint } }));
    if (fpStateRes.ok) {
      const fpState = await fpStateRes.json();
      refinedLabel = fpState.jsExecuted ? "[TH]" : "[SH]";
    } else {
      logBuffer.push(`[DO_ERROR] Failed to get FP state for ${fingerprint}. Status: ${fpStateRes.status}. Treating as SH.`);
      refinedLabel = "[SH]";
    }
  }

  logBuffer.push(`${refinedLabel} ${request.url} IP=${ip} UA=${ua} FP=${fingerprint}`);

  if (refinedLabel === "[TH]" || refinedLabel === "[SAFE_BOT]") {
    return fetch(request);
  }

  if (refinedLabel === "[B]") {
    if (learnedBadBotsCache === null) {
      const learnedList = await env.BOT_BLOCKER_KV.get("LEARNED_BAD_BOTS", { type: "json" });
      learnedBadBotsCache = new Set(Array.isArray(learnedList) ? learnedList : []);
    }
    for (const patt of learnedBadBotsCache) {
      // ★改善点1: アンカー剥がしを削除
      if (new RegExp(patt, "i").test(ua)) {
        const reason = `unwanted-bot(learned):${patt}`;
        ctx.waitUntil(handleViolationSideEffects(ip, ua, reason, 1, env, ctx, fingerprint, 1, logBuffer));
        return new Response("Not Found", { status: 404 });
      }
    }
    
    if (badBotDictionaryCache === null) {
      const listJson = await env.BOT_BLOCKER_KV.get("SYSTEM_BAD_BOT_LIST");
      badBotDictionaryCache = listJson ? JSON.parse(listJson) : [];
    }
    for (const patt of badBotDictionaryCache) {
      // ★改善点1: アンカー剥がしを削除
      if (new RegExp(patt, "i").test(ua)) {
        const reason = `unwanted-bot(new):${patt}`;
        ctx.waitUntil((async () => {
          activeBadBotListCache.add(patt);
          await env.BOT_BLOCKER_KV.put("ACTIVE_BAD_BOT_LIST", JSON.stringify(Array.from(activeBadBotListCache)));
        })());
        ctx.waitUntil(handleViolationSideEffects(ip, ua, reason, 1, env, ctx, fingerprint, 1, logBuffer));
        return new Response("Not Found", { status: 404 });
      }
    }
  }
  
  if (refinedLabel === "[SH]") {
    // ★改善点3: チャレンジはHTMLナビゲーションに限定
    const accept = request.headers.get('Accept') || '';
    if (!accept.includes('text/html')) {
        logBuffer.push('[CHALLENGE SKIP] Non-HTML request');
        return fetch(request);
    }

    // ★改善点3: Turnstile成功後の通行許可クッキーをチェック
    const bypassCookie = (request.headers.get("Cookie")||"").split(/;\s*/).find(c=>c.startsWith("ts_pass="));
    if (bypassCookie && await verifySignedPass(bypassCookie.split("=")[1], env.TURNSTILE_HMAC_SECRET)) {
        logBuffer.push("[TURNSTILE BYPASS] Valid pass cookie found. Allowing request.");
        return fetch(request);
    }
    
    // ★改善点4: POSTリクエストではチャレンジを出さない
    if (request.method !== "GET") {
        logBuffer.push(`[CHALLENGE SKIP] Skipping challenge for non-GET request method: ${request.method}`);
        return fetch(request);
    }
    
    // ★改善点6: WORKER_CONFIGのホットリロード
    const kvConfig = await env.BOT_BLOCKER_KV.get("WORKER_CONFIG", { type: "json" });
    if (workerConfigCache === null || workerConfigCache.version !== kvConfig.version) {
        workerConfigCache = kvConfig;
        logBuffer.push(`[CONFIG] Hot reloaded worker configuration to version ${kvConfig.version}.`);
    }
    const config = workerConfigCache;
    let score = 0;
    const signals = [];

    const secChUa = request.headers.get('Sec-Ch-Ua');
    const acceptLanguage = request.headers.get('Accept-Language');
    if (!secChUa && !acceptLanguage) {
      score += config.scores?.missingHeadersFull || 20;
      signals.push("missing_headers_full");
    } else if (!secChUa || !acceptLanguage) {
      score += config.scores?.missingHeadersPartial || 10;
      signals.push("missing_headers_partial");
    }

    const [ipLocaleRes, fpLocaleRes] = await Promise.all([
        ipTrackerStub.fetch(new Request("https://internal/check-locale", { method: 'POST', headers: { "CF-Connecting-IP": ip, "Content-Type": "application/json" }, body: JSON.stringify({ path }) })),
        fpTrackerStub.fetch(new Request("https://internal/check-locale-fp", { method: 'POST', headers: { "X-Fingerprint-ID": fingerprint, "Content-Type": "application/json" }, body: JSON.stringify({ path }) }))
    ]);
    if ((ipLocaleRes.ok && (await ipLocaleRes.json()).violation) || (fpLocaleRes.ok && (await fpLocaleRes.json()).violation)) {
        score += config.scores?.localeFanout || 20;
        signals.push("locale_fanout");
    }

    logBuffer.push(`[SH_SCORE] Score: ${score} | Signals: [${signals.join(', ')}]`);

    if (score >= (config.thresholds?.challenge || 40)) {
        logBuffer.push(`[TURNSTILE CHALLENGE] Triggered by score ${score} for IP=${ip}`);
        return presentTurnstileChallenge(request, env);
    }
  }
  
  if (ua.startsWith("AmazonProductDiscovery/1.0")) {
    const isVerified = await verifyBotIp(ip, "amazon", env, logBuffer);
    if (!isVerified) {
      const reason = "amazon-impersonation";
      ctx.waitUntil(handleViolationSideEffects(ip, ua, reason, 1, env, ctx, fingerprint, 1, logBuffer));
      return new Response("Not Found", { status: 404 });
    }
  }

  return fetch(request);
}


// --- 3. コアヘルパー関数 ---
async function handleTurnstileVerification(request, env) {
    const url = new URL(request.url);
    const redirectUrl = url.searchParams.get('redirect_to');
    const formData = await request.formData();
    const token = formData.get('cf-turnstile-response');
    const ip = request.headers.get('CF-Connecting-IP');

    let validationResponse = await fetch('https://challenges.cloudflare.com/turnstile/v0/siteverify', {
        method: 'POST',
        headers: {'content-type': 'application/json'},
        body: JSON.stringify({ secret: env.TURNSTILE_SECRET_KEY, response: token, remoteip: ip }),
    });
    const outcome = await validationResponse.json();

    if (outcome.success) {
        // ★改善点3: 署名付き通行許可クッキーを発行
        const pass = await issueSignedPass(env.TURNSTILE_HMAC_SECRET, 10); // 10分有効
        const headers = new Headers();
        headers.set('Set-Cookie', `ts_pass=${pass}; Max-Age=${10*60}; Path=/; HttpOnly; Secure; SameSite=Lax`);
        if (redirectUrl) {
            headers.set('Location', redirectUrl);
            return new Response(null, { status: 302, headers });
        }
        return new Response("OK", { status: 200, headers });
    }
    
    return new Response("Human verification failed. Please try again.", { status: 403, headers: { 'Content-Type': 'text/plain; charset=utf-8' } });
}

function presentTurnstileChallenge(request, env) {
    const originalUrl = request.url;
    const siteKey = env.TURNSTILE_SITE_KEY;
    const html = `
      <!DOCTYPE html><html lang="ja"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>接続を確認しています...</title><script src="https://challenges.cloudflare.com/turnstile/v0/api.js" async defer></script>
      <meta name="robots" content="noindex,nofollow">
      <style>body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;display:flex;justify-content:center;align-items:center;height:100vh;margin:0;background-color:#f1f2f3;color:#333;}.container{text-align:center;padding:2em;background-color:white;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.1);}h1{font-size:1.2em;margin-bottom:0.5em;}p{margin-top:0;color:#666;}</style>
      </head><body><div class="container"><h1>接続が安全であることを確認しています</h1><p>この処理は自動で行われます。しばらくお待ちください。</p>
      <form id="turnstile-form" action="/cf-turnstile/verify?redirect_to=${encodeURIComponent(originalUrl)}" method="POST">
      <div class="cf-turnstile" data-sitekey="${siteKey}" data-callback="onTurnstileSuccess"></div></form></div>
      <script>function onTurnstileSuccess(token){document.getElementById('turnstile-form').submit();}</script>
      </body></html>`;
    return new Response(html, { headers: { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' } });
}

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
function isAdminPath(p){ return p.startsWith("/admin/") || p.startsWith("/reset-state") || p.startsWith("/debug/"); }
function isAuthorizedAdmin(req, env){
  const cookie = req.headers.get('Cookie')||'';
  return cookie.includes(`admin_key=${env.ADMIN_KEY}`);
}

async function issueSignedPass(secret, minutes){
  const exp = Date.now() + minutes*60*1000;
  // ★改善点1: HMACのハッシュ指定を修正
  const key = await crypto.subtle.importKey("raw", new TextEncoder().encode(secret), {name:"HMAC", hash:"SHA-256"}, false, ["sign"]);
  const sigBuf = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(String(exp)));
  // ★改善点2: TurnstileクッキーをBase64URLに
  const raw = String.fromCharCode(...new Uint8Array(sigBuf));
  const sig = btoa(raw).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');
  return `${exp}.${sig}`;
}
async function verifySignedPass(token, secret){
  const [exp, sig] = token.split('.',2);
  if (!exp || !sig || Date.now()>Number(exp)) return false;
  const key = await crypto.subtle.importKey("raw", new TextEncoder().encode(secret), {name:"HMAC", hash:"SHA-256"}, false, ["sign"]);
  const sigBuf = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(String(exp)));
  // ★改善点2: TurnstileクッキーをBase64URLに
  const raw = String.fromCharCode(...new Uint8Array(sigBuf));
  const expect = btoa(raw).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');
  return sig === expect;
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
