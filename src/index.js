/*
 * =================================================================
 * 目次 (Table of Contents)
 * =================================================================
 * 1. エクスポートとメインハンドラ (Exports & Main Handlers)
 * 2. メインロジック (Main Logic)
 * 3. コアヘルパー関数 (Core Helper Functions)
 * 4. ユーティリティ関数 (Utility Functions)
 * =================================================================
 *
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
// src/index.js
/*
 * =================================================================
 * 目次 (Table of Contents)
 * =================================================================
 * 1. imports / exports / module-scope caches
 * 2. KV cache helpers (read/write debounce)
 * 3. Durable Object safe wrapper
 * 4. FP "JS executed" tracking (KV-based)
 * 5. Worker entrypoints (fetch / scheduled)
 * 6. Main request handler: handle()
 * 7. Turnstile handlers
 * 8. Violation handling (KV/R2)
 * 9. Bot verification (CIDR)
 * 10. Utilities (admin, cookies, token, cidr)
 * =================================================================
 */

import { IPStateTrackerV2 } from "./do/IPStateTracker.js";
import { FingerprintTrackerV2, generateFingerprint } from "./do/FingerprintTracker.js";

export { IPStateTrackerV2 };
export { FingerprintTrackerV2 };

/* -----------------------------------------------------------------
 * 1) imports / exports / module-scope caches
 * ----------------------------------------------------------------- */

// キャッシュ（モジュールスコープ）
let workerConfigCache = null;
let learnedBadBotsCache = null;
let badBotDictionaryCache = null;
let activeBadBotListCache = null;
let activeBadBotListLastRead = 0;
let asnBlocklistCache = null;

/* -----------------------------------------------------------------
 * 2) KV cache helpers (read/write debounce)
 * ----------------------------------------------------------------- */

// --- KV読み Cache API / メモリ短期キャッシュ + ネガティブキャッシュ ---
const __memCache = new Map(); // key -> { val, exp }
const __negCache = new Map(); // key -> expiration(ms)
const NEGATIVE_CACHE_TTL_MS = 10 * 60 * 1000; // 10分

// 2-1) getBlockStatusCached(): KVの状態を Cache API + mem + negative cache で高速化
async function getBlockStatusCached(env, key) {
  const now = Date.now();

  const negExp = __negCache.get(key);
  if (negExp && negExp > now) return "";

  const m = __memCache.get(key);
  if (m && m.exp > now) return m.val;

  const cache = caches.default;
  const req = new Request("https://kv-cache.local/block/" + encodeURIComponent(key));
  const hit = await cache.match(req);
  if (hit) {
    const val = await hit.text();
    __memCache.set(key, { val, exp: now + 60_000 });
    if (!val) __negCache.set(key, now + NEGATIVE_CACHE_TTL_MS);
    return val;
  }

  const val = (await env.BOT_BLOCKER_KV.get(key, { cacheTtl: 300 })) || "";
  await cache.put(req, new Response(val, { headers: { "Cache-Control": "max-age=300" } }));
  __memCache.set(key, { val, exp: now + 60_000 });
  if (!val) __negCache.set(key, now + NEGATIVE_CACHE_TTL_MS);
  return val;
}

// --- KV 書き込みデバウンス ---
const __recentPuts = new Map(); // key -> exp

// 2-2) putOnce(): 同一キーの連続KV putを抑止
async function putOnce(env, key, val, ttlSeconds) {
  const now = Date.now();
  if (__recentPuts.get(key) > now) return;
  __recentPuts.set(key, now + 30_000);

  const opts = {};
  if (typeof ttlSeconds === "number") opts.expirationTtl = ttlSeconds;
  await env.BOT_BLOCKER_KV.put(key, val, opts);
}

/* -----------------------------------------------------------------
 * 3) Durable Object safe wrapper
 * ----------------------------------------------------------------- */

// 3-1) safeFetchDO(): DOが死んでいても Worker 全体を落とさない
async function safeFetchDO(stub, req, logBuffer, tag) {
  try {
    return await stub.fetch(req);
  } catch (e) {
    if (logBuffer) logBuffer.push(`[DO_FAIL] ${tag} ${String(e?.message || e)}`);
    return null;
  }
}

/* -----------------------------------------------------------------
 * 4) FP "JS executed" tracking (KV-based)
 * ----------------------------------------------------------------- */

const FPJS_NS = "FPJS:"; // FPJS:<fingerprint> = "1" TTL

// 4-1) isJsExecuted(): KVから「JS実行済み」判定
async function isJsExecuted(env, fingerprint) {
  const v = await env.BOT_BLOCKER_KV.get(FPJS_NS + fingerprint, { cacheTtl: 300 });
  return v === "1";
}

// 4-2) markJsExecuted(): KVへ「JS実行済み」記録（TTL）
async function markJsExecuted(env, fingerprint) {
  // 24h保持（必要なら短くしてOK）
  await putOnce(env, FPJS_NS + fingerprint, "1", 24 * 3600);
}

/* -----------------------------------------------------------------
 * 5) Worker entrypoints (fetch / scheduled)
 * ----------------------------------------------------------------- */

export default {
  // 5-1) fetch entry
  async fetch(request, env, ctx) {
    const logBuffer = [];
    try {
      return await handle(request, env, ctx, logBuffer);
    } finally {
      for (const message of logBuffer) console.log(message);
      console.log("----------------------------------------");
    }
  },

  // 5-2) scheduled entry (cron)
  async scheduled(event, env, ctx) {
    console.log("Cron Trigger fired: Syncing lists...");

    const object = await env.BLOCKLIST_R2.get("dictionaries/bad-bots.txt");
    if (object) {
      const text = await object.text();
      const list = text.split("\n").filter((line) => line && !line.startsWith("#"));
      await env.BOT_BLOCKER_KV.put("SYSTEM_BAD_BOT_LIST", JSON.stringify(list));
      console.log(`Synced ${list.length} bad bot patterns from R2 to KV.`);
    } else {
      console.error("Failed to get bad-bots.txt from R2.");
    }
  },
};

/* -----------------------------------------------------------------
 * 6) Main request handler: handle()
 * ----------------------------------------------------------------- */

// 6-1) handle(): すべてのリクエストを振り分けるメイン関数
async function handle(request, env, ctx, logBuffer) {
  const url = new URL(request.url);

  // Turnstile verify endpoint
  if (url.pathname === "/cf-turnstile/verify") {
    return await handleTurnstileVerification(request, env);
  }

  // Admin path
  if (isAdminPath(url.pathname)) {
    if (!isAuthorizedAdmin(request, env)) return new Response("Not Found", { status: 404 });
    return new Response("Admin action completed.", { status: 200 });
  }

  const ua = request.headers.get("User-Agent") || "UA_NOT_FOUND";
  const ip = request.headers.get("CF-Connecting-IP") || "IP_NOT_FOUND";
  const path = url.pathname.toLowerCase();
  const fingerprint = await generateFingerprint(request, logBuffer);

  // 1) Cookie whitelist
  const cookieHeader = request.headers.get("Cookie") || "";
  if (cookieHeader.includes("secret-pass=Rocaniru-Admin-Bypass-XYZ789")) {
    logBuffer.push(`[WHITELIST] Access granted via secret cookie for IP=${ip}`);
    return fetch(request);
  }

  // 2) アセットは即返す（JSだけは「実行済み」マークをKVに）
  const EXT_SKIP =
    /\.(jpg|jpeg|png|gif|svg|webp|js|css|woff2?|ttf|ico|map|txt|eot|otf|json|xml|avif)(\?|$)/;
  if (EXT_SKIP.test(path)) {
    const importantJsPatterns = [
      /^\/\.well-known\/shopify\/monorail\//,
      /^\/\.well-known\/shopify\/monorail\/unstable\/produce_batch/,
      /^\/cdn\/shopifycloud\/privacy-banner\/storefront-banner\.js/,
      /^\/cart\.js/,
      /^\/cdn\/shop\/t\/\d+\/assets\/theme\.min\.js(\?|$)/,
    ];

    if (importantJsPatterns.some((pattern) => pattern.test(path))) {
      // DOではなくKVで「JS実行済み」を記録
      ctx.waitUntil(markJsExecuted(env, fingerprint));
    }
    return fetch(request);
  }

  // 3) KV ブロック状態チェック
  const [ipStatus, fpStatus] = await Promise.all([
    getBlockStatusCached(env, ip),
    getBlockStatusCached(env, `FP-${fingerprint}`),
  ]);

  if (["permanent-block", "temp-1", "temp-2", "temp-3"].includes(ipStatus)) {
    logBuffer.push(`[KV BLOCK] IP=${ip} status=${ipStatus}`);
    return new Response("Not Found", { status: 404 });
  }
  if (["permanent-block", "temp-1", "temp-2", "temp-3"].includes(fpStatus)) {
    logBuffer.push(`[KV BLOCK] FP=${fingerprint} status=${fpStatus}`);
    return new Response("Not Found", { status: 404 });
  }

  // 4) アクティブ悪質ボットリスト（10分おき再読込）
  const now = Date.now();
  if (activeBadBotListCache === null || now - activeBadBotListLastRead > 600000) {
    const listJson = await env.BOT_BLOCKER_KV.get("ACTIVE_BAD_BOT_LIST");
    activeBadBotListCache = new Set(listJson ? JSON.parse(listJson) : []);
    activeBadBotListLastRead = now;
    logBuffer.push("[CONFIG] Reloaded active bad bot list from KV.");
  }

  for (const patt of activeBadBotListCache) {
    try {
      if (new RegExp(patt, "i").test(ua)) {
        logBuffer.push(`[ACTIVE BAD BOT BLOCK] UA matched active list rule: ${patt}`);
        return new Response("Forbidden", { status: 403 });
      }
    } catch {
      logBuffer.push(`[REGEX_ERROR] Invalid pattern in ACTIVE_BAD_BOT_LIST: ${patt}`);
    }
  }

  // 5) 静的ルール：パス探索型攻撃を即ブロック
  const staticBlockPatterns = [
    "/wp-",
    ".php",
    "phpinfo",
    "phpmyadmin",
    "/.env",
    "/config",
    "/admin/",
    "/dbadmin",
    "/_profiler",
    ".aws",
    "credentials",
  ];
  if (staticBlockPatterns.some((patt) => path.includes(patt))) {
    return logAndBlock(ip, ua, "path-scan", env, ctx, fingerprint, logBuffer);
  }

  // 6) UAベース判定
  const safeBotPatterns = ["PetalBot"];
  const botPattern =
    /\b(\w+bot|bot|crawl(er)?|spider|slurp|fetch|headless|preview|agent|scanner|client|curl|wget|python|perl|java|scrape(r)?|monitor|probe|archive|validator|feed)\b/i;

  let refinedLabel = "[H]";

  // Safe-bot rate limit のために DO スタブは作る（ただしDOが壊れても落ちない）
  const ipTrackerStub = env.IP_STATE_TRACKER.get(env.IP_STATE_TRACKER.idFromName(ip));
  const fpTrackerStub = env.FINGERPRINT_TRACKER.get(env.FINGERPRINT_TRACKER.idFromName(fingerprint));

  if (botPattern.test(ua)) {
    refinedLabel = "[B]";

    if (safeBotPatterns.some((safeBot) => ua.toLowerCase().includes(safeBot.toLowerCase()))) {
      // SafeBotだけは軽くレート制限（DOが死んでたら許可）
      const res = await safeFetchDO(
        ipTrackerStub,
        new Request("https://internal/rate-limit", { headers: { "CF-Connecting-IP": ip } }),
        logBuffer,
        "rate-limit"
      );
      if (res && res.ok) {
        const { allowed } = await res.json();
        if (!allowed) {
          logBuffer.push(`[RATE LIMIT] SafeBot blocked. IP=${ip}`);
          return new Response("Too Many Requests", { status: 429 });
        }
      }
      refinedLabel = "[SAFE_BOT]";
    }
  }

  // 7) TH/SH 判定：DOではなく KV（FPJS）で判断
  if (refinedLabel === "[H]") {
    const jsOk = await isJsExecuted(env, fingerprint);
    refinedLabel = jsOk ? "[TH]" : "[SH]";
  }

  logBuffer.push(`${refinedLabel} ${request.url} IP=${ip} UA=${ua} FP=${fingerprint}`);

  // TH / SAFE_BOT は通す
  if (refinedLabel === "[TH]" || refinedLabel === "[SAFE_BOT]") {
    return fetch(request);
  }

  // 8) B判定：学習済み/辞書
  if (refinedLabel === "[B]") {
    if (learnedBadBotsCache === null) {
      const learnedList = await env.BOT_BLOCKER_KV.get("LEARNED_BAD_BOTS", { type: "json" });
      learnedBadBotsCache = new Set(Array.isArray(learnedList) ? learnedList : []);
    }

    for (const patt of learnedBadBotsCache) {
      try {
        if (new RegExp(patt, "i").test(ua)) {
          const reason = `unwanted-bot(learned):${patt}`;
          ctx.waitUntil(handleViolationSideEffects(ip, ua, reason, 1, env, ctx, fingerprint, 1, logBuffer));
          return new Response("Not Found", { status: 404 });
        }
      } catch {
        logBuffer.push(`[REGEX_ERROR] Invalid pattern in LEARNED_BAD_BOTS: ${patt}`);
      }
    }

    if (badBotDictionaryCache === null) {
      const listJson = await env.BOT_BLOCKER_KV.get("SYSTEM_BAD_BOT_LIST");
      badBotDictionaryCache = listJson ? JSON.parse(listJson) : [];
    }

    for (const patt of badBotDictionaryCache) {
      try {
        if (new RegExp(patt, "i").test(ua)) {
          const reason = `unwanted-bot(new):${patt}`;

          if (activeBadBotListCache) {
            activeBadBotListCache.add(patt);
            ctx.waitUntil(
              env.BOT_BLOCKER_KV.put(
                "ACTIVE_BAD_BOT_LIST",
                JSON.stringify(Array.from(activeBadBotListCache))
              )
            );
          }

          ctx.waitUntil(handleViolationSideEffects(ip, ua, reason, 1, env, ctx, fingerprint, 1, logBuffer));
          return new Response("Not Found", { status: 404 });
        }
      } catch {
        logBuffer.push(`[REGEX_ERROR] Invalid pattern in SYSTEM_BAD_BOT_LIST: ${patt}`);
      }
    }
  }

  // 9) SH判定：Turnstile条件（locale fanout は DOのメモリで見る）
  if (refinedLabel === "[SH]") {
    const accept = request.headers.get("Accept") || "";
    const isHtmlRequest = accept.includes("text/html") || accept.includes("*/*") || accept === "";
    if (!isHtmlRequest) {
      logBuffer.push("[CHALLENGE SKIP] Non-HTML request");
      return fetch(request);
    }

    const cookies = parseCookieSafe(request);
    const passToken = cookies["ts_pass"];
    if (passToken && (await checkKvPassToken(env, passToken, fingerprint))) {
      logBuffer.push("[TURNSTILE BYPASS] KV pass token valid. Allowing request.");
      return fetch(request);
    }

    if (request.method !== "GET") {
      logBuffer.push(`[CHALLENGE SKIP] non-GET: ${request.method}`);
      return fetch(request);
    }

    const kvConfig = (await env.BOT_BLOCKER_KV.get("WORKER_CONFIG", { type: "json" })) ?? {};
    if (workerConfigCache === null || (kvConfig && workerConfigCache.version !== kvConfig.version)) {
      workerConfigCache = kvConfig;
      const v = typeof kvConfig?.version !== "undefined" ? kvConfig.version : "none";
      logBuffer.push(`[CONFIG] Hot reloaded worker configuration to version ${v}.`);
    }
    const config = workerConfigCache;

    let score = 0;
    const signals = [];

    const asn = request.cf?.asn;
    if (asn) {
      if (asnBlocklistCache === null) {
        const blocklistJson = await env.BOT_BLOCKER_KV.get("ASN_BLOCKLIST");
        asnBlocklistCache = blocklistJson ? JSON.parse(blocklistJson) : [];
      }
      if (asnBlocklistCache.includes(String(asn))) {
        score += config?.scores?.suspiciousAsn ?? 25;
        signals.push(`suspicious_asn:${asn}`);
      }
    }

    const secChUa = request.headers.get("Sec-Ch-Ua");
    const acceptLanguage = request.headers.get("Accept-Language");
    if (!secChUa && !acceptLanguage) {
      score += config?.scores?.missingHeadersFull ?? 20;
      signals.push("missing_headers_full");
    } else if (!secChUa || !acceptLanguage) {
      score += config?.scores?.missingHeadersPartial ?? 10;
      signals.push("missing_headers_partial");
    }

    // locale fanout（DOが死んでたら違反なし扱い）
    const [ipLocaleRes, fpLocaleRes] = await Promise.all([
      safeFetchDO(
        ipTrackerStub,
        new Request("https://internal/check-locale", {
          method: "POST",
          headers: { "CF-Connecting-IP": ip, "Content-Type": "application/json" },
          body: JSON.stringify({ path, config, country: request.cf?.country }),
        }),
        logBuffer,
        "check-locale-ip"
      ),
      safeFetchDO(
        fpTrackerStub,
        new Request("https://internal/check-locale-fp", {
          method: "POST",
          headers: { "X-Fingerprint-ID": fingerprint, "Content-Type": "application/json" },
          body: JSON.stringify({ path, config, country: request.cf?.country }),
        }),
        logBuffer,
        "check-locale-fp"
      ),
    ]);

    let localeViolation = false;
    try {
      if (ipLocaleRes && ipLocaleRes.ok) localeViolation = localeViolation || !!(await ipLocaleRes.json()).violation;
    } catch {}
    try {
      if (fpLocaleRes && fpLocaleRes.ok) localeViolation = localeViolation || !!(await fpLocaleRes.json()).violation;
    } catch {}

    if (localeViolation) {
      score += config?.scores?.localeFanout ?? 20;
      signals.push("locale_fanout");
    }

    logBuffer.push(`[SH_SCORE] Score: ${score} | Signals: [${signals.join(", ")}]`);

    if (score >= (config?.thresholds?.challenge ?? 40)) {
      logBuffer.push(`[TURNSTILE CHALLENGE] score=${score} IP=${ip}`);
      return presentTurnstileChallenge(request, env, fingerprint);
    }
  }

  // Amazon UA（なりすましチェック）
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

/* -----------------------------------------------------------------
 * 7) Turnstile handlers
 * ----------------------------------------------------------------- */

// 7-1) handleTurnstileVerification(): Turnstileの検証とts_pass発行
async function handleTurnstileVerification(request, env) {
  const url = new URL(request.url);
  const redirectUrl = url.searchParams.get("redirect_to");
  const formData = await request.formData();
  const token = formData.get("cf-turnstile-response");
  const fingerprint = formData.get("fp");
  const ip = request.headers.get("CF-Connecting-IP");

  const validationResponse = await fetch("https://challenges.cloudflare.com/turnstile/v0/siteverify", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ secret: env.TURNSTILE_SECRET_KEY, response: token, remoteip: ip }),
  });

  const outcome = await validationResponse.json();

  if (outcome.success) {
    const cookieStr = await issueKvPassToken(env, fingerprint, 10 * 60);
    const headers = new Headers();
    headers.set("Set-Cookie", cookieStr);
    if (redirectUrl) {
      headers.set("Location", redirectUrl);
      return new Response(null, { status: 302, headers });
    }
    return new Response("OK", { status: 200, headers });
  }

  return new Response("Human verification failed. Please try again.", {
    status: 403,
    headers: { "Content-Type": "text/plain; charset=utf-8" },
  });
}

// 7-2) presentTurnstileChallenge(): challenge HTMLを返す
function presentTurnstileChallenge(request, env, fingerprint) {
  const originalUrl = request.url;
  const siteKey = env.TURNSTILE_SITE_KEY;

  const html = `<!DOCTYPE html><html lang="ja"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>接続を確認しています...</title><script src="https://challenges.cloudflare.com/turnstile/v0/api.js" async defer></script>
<meta name="robots" content="noindex,nofollow">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src https://challenges.cloudflare.com; connect-src https://challenges.cloudflare.com; frame-src https://challenges.cloudflare.com; style-src 'unsafe-inline'; base-uri 'none'; form-action 'self'">
<style>body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;display:flex;justify-content:center;align-items:center;height:100vh;margin:0;background-color:#f1f2f3;color:#333;}.container{text-align:center;padding:2em;background-color:white;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.1);}h1{font-size:1.2em;margin-bottom:0.5em;}p{margin-top:0;color:#666;}</style>
</head><body><div class="container"><h1>接続が安全であることを確認しています</h1><p>この処理は自動で行われます。しばらくお待ちください。</p>
<form id="turnstile-form" action="/cf-turnstile/verify?redirect_to=${encodeURIComponent(originalUrl)}" method="POST">
<input type="hidden" name="fp" value="${fingerprint}">
<div class="cf-turnstile" data-sitekey="${siteKey}" data-callback="onTurnstileSuccess"></div></form></div>
<script>function onTurnstileSuccess(token){document.getElementById('turnstile-form').submit();}</script>
</body></html>`;

  return new Response(html, {
    headers: {
      "Content-Type": "text/html; charset=utf-8",
      "Cache-Control": "no-store",
      "Referrer-Policy": "no-referrer",
    },
  });
}

/* -----------------------------------------------------------------
 * 8) Violation handling (KV/R2)
 * ----------------------------------------------------------------- */

// 8-1) handleViolationSideEffects(): KVブロック/R2記録などの副作用をまとめて実行
async function handleViolationSideEffects(ip, ua, reason, ipCount, env, ctx, fingerprint, fpCount, logBuffer) {
  logBuffer.push(`[VIOLATION] IP=${ip} FP=${fingerprint} reason=${reason} IP_count=${ipCount} FP_count=${fpCount}`);

  const effectiveCount = Math.max(ipCount, fpCount);

  if (effectiveCount === 1) {
    ctx.waitUntil(putOnce(env, ip, "temp-1", 600));
    ctx.waitUntil(putOnce(env, `FP-${fingerprint}`, "temp-1", 600));
  } else if (effectiveCount === 2) {
    ctx.waitUntil(putOnce(env, ip, "temp-2", 1800));
    ctx.waitUntil(putOnce(env, `FP-${fingerprint}`, "temp-2", 1800));
  } else if (effectiveCount === 3) {
    ctx.waitUntil(putOnce(env, ip, "temp-3", 24 * 3600));
    ctx.waitUntil(putOnce(env, `FP-${fingerprint}`, "temp-3", 24 * 3600));
  } else if (effectiveCount >= 4) {
    ctx.waitUntil(putOnce(env, ip, "permanent-block"));
    ctx.waitUntil(putOnce(env, `FP-${fingerprint}`, "permanent-block"));

    const record = JSON.stringify({
      ip,
      fingerprint,
      userAgent: ua,
      reason,
      ipCount,
      fpCount,
      timestamp: new Date().toISOString(),
    });

    ctx.waitUntil(env.BLOCKLIST_R2.put(`${ip}-${fingerprint.substring(0, 8)}-${Date.now()}.json`, record));
    ctx.waitUntil(putOnce(env, `FP-HIGH-COUNT-${fingerprint}`, "pending-permanent-block", 24 * 3600));
  }
}

// 8-2) logAndBlock(): 即ブロック系のショートカット
function logAndBlock(ip, ua, reason, env, ctx, fingerprint, logBuffer) {
  ctx.waitUntil(handleViolationSideEffects(ip, ua, reason, 1, env, ctx, fingerprint, 1, logBuffer));
  return new Response("Not Found", { status: 404 });
}

/* -----------------------------------------------------------------
 * 9) Bot verification (CIDR)
 * ----------------------------------------------------------------- */

// 9-1) verifyBotIp(): BOT_CIDRS(KV)のCIDRに含まれるか
async function verifyBotIp(ip, botKey, env, logBuffer) {
  const botCidrsCache = await env.BOT_BLOCKER_KV.get("BOT_CIDRS", { type: "json", cacheTtl: 3600 });
  const cidrs = botCidrsCache ? botCidrsCache[botKey] : null;

  if (!cidrs || !Array.isArray(cidrs) || cidrs.length === 0) {
    logBuffer.push(`[WARN] CIDR list for bot '${botKey}' is empty or not found in KV.`);
    return false;
  }
  return cidrs.some((cidr) => ipInCidr(ip, cidr, logBuffer));
}

/* -----------------------------------------------------------------
 * 10) Utilities (admin, cookies, token, cidr)
 * ----------------------------------------------------------------- */

// 10-1) isAdminPath(): 管理用パス判定
function isAdminPath(p) {
  return p.startsWith("/admin/") || p.startsWith("/reset-state") || p.startsWith("/debug/");
}

// 10-2) parseCookieSafe(): Cookieを安全にパース
function parseCookieSafe(req) {
  const header = req.headers.get("Cookie") || "";
  const map = Object.create(null);
  for (const part of header.split(/;\s*/)) {
    if (!part) continue;
    const i = part.indexOf("=");
    if (i < 1) continue;
    try {
      const k = decodeURIComponent(part.slice(0, i).trim());
      const v = decodeURIComponent(part.slice(i + 1).trim());
      map[k] = v;
    } catch {}
  }
  return map;
}

// 10-3) constantTimeEqual(): タイミング攻撃対策の比較
function constantTimeEqual(a, b) {
  if (typeof a !== "string" || typeof b !== "string" || a.length !== b.length) return false;
  let r = 0;
  for (let i = 0; i < a.length; i++) r |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return r === 0;
}

// 10-4) isAuthorizedAdmin(): admin_key cookie と env.ADMIN_KEY を比較
function isAuthorizedAdmin(req, env) {
  const c = parseCookieSafe(req);
  const val = c["admin_key"];
  return val ? constantTimeEqual(val, env.ADMIN_KEY) : false;
}

// 10-5) base64url(): URL-safe base64
function base64url(bytes) {
  let s = btoa(String.fromCharCode(...bytes));
  return s.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

// 10-6) generateToken(): ランダムトークン生成
function generateToken(len = 32) {
  const buf = new Uint8Array(len);
  crypto.getRandomValues(buf);
  return base64url(buf);
}

const PASS_NS = "PASS:";

// 10-7) issueKvPassToken(): Turnstile通過用 ts_pass をKVへ発行
async function issueKvPassToken(env, fingerprint, ttlSeconds = 600) {
  const token = generateToken();
  const value = JSON.stringify({ fp: fingerprint, iat: Date.now() });
  await env.BOT_BLOCKER_KV.put(PASS_NS + token, value, { expirationTtl: ttlSeconds });
  return `ts_pass=${token}; Max-Age=${ttlSeconds}; Path=/; HttpOnly; Secure; SameSite=Lax`;
}

// 10-8) checkKvPassToken(): ts_pass を検証
async function checkKvPassToken(env, token, fingerprint) {
  if (!token) return false;
  const raw = await env.BOT_BLOCKER_KV.get(PASS_NS + token, { cacheTtl: 60 });
  if (!raw) return false;
  try {
    const data = JSON.parse(raw);
    if (data?.fp === fingerprint) return true;
  } catch {}
  return false;
}

// 10-9) ipToBigInt(): IPv4/IPv6をBigIntへ
function ipToBigInt(ip) {
  if (ip.includes(":")) {
    const parts = ip.split("::");
    let part1 = [],
      part2 = [];
    if (parts.length > 1) {
      part1 = parts[0].split(":").filter((p) => p.length > 0);
      part2 = parts[1].split(":").filter((p) => p.length > 0);
    } else {
      part1 = ip.split(":");
    }
    const zeroGroups = 8 - (part1.length + part2.length);
    const full = [...part1, ...Array(zeroGroups).fill("0"), ...part2];
    return full.reduce((acc, p) => (acc << 16n) + BigInt(`0x${p || "0"}`), 0n);
  } else {
    return ip.split(".").reduce((acc, p) => (acc << 8n) + BigInt(p), 0n);
  }
}

// 10-10) ipInCidr(): CIDR一致判定
function ipInCidr(ip, cidr, logBuffer) {
  try {
    const [base, prefixStr] = cidr.split("/");
    const prefix = parseInt(prefixStr, 10);
    const isV6 = cidr.includes(":");
    const totalBits = isV6 ? 128 : 32;

    if (isNaN(prefix) || prefix < 0 || prefix > totalBits) return false;
    if (isV6 !== ip.includes(":")) return false;

    const ipVal = ipToBigInt(ip);
    const baseVal = ipToBigInt(base);
    const mask = ((1n << BigInt(prefix)) - 1n) << BigInt(totalBits - prefix);
    return (ipVal & mask) === (baseVal & mask);
  } catch (e) {
    logBuffer.push(`[ipInCidr_ERROR] ip='${ip}' cidr='${cidr}' msg='${e?.message || e}'`);
    return false;
  }
}
