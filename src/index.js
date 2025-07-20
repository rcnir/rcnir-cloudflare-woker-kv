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
 * 便利なコマンド (Useful Commands)
 * =================================================================
 *
 * H判定（人間）のログをリアルタイム表示:
 * npx wrangler tail shopify-bot-blocker | grep -F "[H]"
 *
 * B判定（ボット）のログをリアルタイム表示:
 * npx wrangler tail shopify-bot-blocker | grep -F "[B]"
 *
 * 違反が検知されたログだけを表示:
 * npx wrangler tail shopify-bot-blocker | grep -F "[VIOLATION]"
 *
 * KVの情報に基づいてブロックしたログだけを表示:
 * npx wrangler tail shopify-bot-blocker | grep -F "[KV BLOCK]"
 *
 * 特定IPのブロック状態を確認 (例: 192.0.2.1):
 * npx wrangler kv:key get --namespace-id="7da99382fc3945bd87bc65f55c9ea1fb" "192.0.2.1"
 *
 * 永続ブロックされたIPの全ログをR2で一覧表示:
 * npx wrangler r2 object list rocaniiru-log
 *
 * =================================================================
 */

// --- 1. エクスポートとメインハンドラ ---

import { IPStateTracker } from "./do/IPStateTracker.js";
// ★変更: FingerprintTracker DOとフィンガープリント生成関数をインポート★
import { FingerprintTracker, generateFingerprint } from "./do/FingerprintTracker.js";

export { IPStateTracker };
export { FingerprintTracker }; // ★変更: Durable Objectとして公開するために必要★

let botCidrsCache = null;
let unwantedBotPatternsCache = null;
let learnedBadBotsCache = null; // Bad Botパターン学習用キャッシュ
let badBotDictionaryCache = null; // Bad Bot辞書用キャッシュ


export default {
  async fetch(request, env, ctx) {
    return handle(request, env, ctx);
  },

  async scheduled(event, env, ctx) {
    console.log("Cron Trigger fired: Syncing permanent block list...");
    const id = env.IP_STATE_TRACKER.idFromName("sync-job"); // Binding名を変更
    const stub = env.IP_STATE_TRACKER.get(id);
    const res = await stub.fetch(new Request("https://internal/list-high-count")); // IP_STATE_TRACKERから高カウントIPを取得
    if (!res.ok) {
      console.error(`Failed to fetch high count IPs from DO. Status: ${res.status}`);
      // エラー時でもFP同期は試行する
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

    // ★変更: FingerprintTrackerから高カウントフィンガープリントを直接KVから取得し同期★
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
        // KVから一時的な"FP-HIGH-COUNT-"エントリを削除
        const deletePromises = allHighCountFpKeys.map(fp => env.BOT_BLOCKER_KV.delete(`FP-HIGH-COUNT-${fp}`));
        await Promise.all(deletePromises);
    } else {
        console.log("No new Fingerprints to permanently block.");
    }
  }
};


// --- 2. メインロジック ---
async function handle(request, env, ctx) {
  const url = new URL(request.url);

  // ★★★ 変更開始: リセットエンドポイントをメインWorkerで直接処理する ★★★
  const resetKey = url.searchParams.get("reset_key");

  // IPStateTrackerの全データをリセット
  if (url.pathname === "/admin/reset-all-violations") {
    if (!env.DO_RESET_KEY || resetKey !== env.DO_RESET_KEY) {
      return new Response("Unauthorized reset attempt.", { status: 401 });
    }
    // IPStateTracker の admin/reset-all-violations を呼び出す
    // IPStateTracker には内部的に deleteAll を呼び出すエンドポイントが必要です
    const ipStateTrackerStub = env.IP_STATE_TRACKER.idFromName("master-reset-key"); // 特定のマスターDO ID
    const ipRes = await env.IP_STATE_TRACKER.get(ipStateTrackerStub).fetch(new Request("https://internal/admin/reset-all-violations", {
        headers: {"X-Reset-Key": resetKey} // DO側で認証のためにキーを渡す
    }));
    
    if (ipRes.ok) {
        return new Response("All IP violation data has been reset.", { status: 200 });
    } else {
        return new Response(`Failed to reset IP violation data: ${ipRes.status} ${await ipRes.text()}`, { status: ipRes.status });
    }
  }

  // FingerprintTrackerの全データをリセット
  if (url.pathname === "/reset-state") {
    if (!env.DO_RESET_KEY || resetKey !== env.DO_RESET_KEY) {
      return new Response("Unauthorized reset attempt.", { status: 401 });
    }
    // FingerprintTrackerも同様に、マスターリセットDO IDにリセット要求を送る
    // FingerprintTracker には内部的に deleteAll を呼び出すエンドポイントが必要です
    const fpTrackerStub = env.FINGERPRINT_TRACKER.idFromName("master-reset-key"); // 特定のマスターDO ID
    const fpRes = await env.FINGERPRINT_TRACKER.get(fpTrackerStub).fetch(new Request("https://internal/reset-state", {
        headers: {"X-Reset-Key": resetKey} // DO側で認証のためにキーを渡す
    }));

    if (fpRes.ok) {
        return new Response("All FingerprintTracker states have been reset.", { status: 200 });
    } else {
        return new Response(`Failed to reset FingerprintTracker data: ${fpRes.status} ${await fpRes.text()}`, { status: fpRes.status });
    }
  }
  // ★★★ 変更終了 ★★★


  const ua = request.headers.get("User-Agent") || "UA_NOT_FOUND";
  const ip = request.headers.get("CF-Connecting-IP") || "IP_NOT_FOUND";
  const path = url.pathname.toLowerCase();
  const fingerprint = await generateFingerprint(request); // FPは常に生成

  // 🔧 **デバッグ用：KVに保存された全ブロックIP/FPを取得**
  if (url.pathname === "/debug/list-blocked-ips") {
    let cursor = undefined;
    const allKeys = [];
    do {
      const listResult = await env.BOT_BLOCKER_KV.list({ limit: 1000, cursor });
      allKeys.push(...listResult.keys.map(k => k.name));
      cursor = listResult.list_complete ? undefined : listResult.cursor;
    } while (cursor);
    return new Response(JSON.stringify(allKeys), {
      headers: { "Content-Type": "application/json" }
    });
  }
  // 🔧 **デバッグ用：特定のフィンガープリントのDO状態を取得**
  if (url.pathname.startsWith("/debug/get-fp-state/")) {
    const targetFingerprint = url.pathname.split("/").pop();
    if (!targetFingerprint) return new Response("Missing fingerprint ID", { status: 400 });

    const fpTrackerId = env.FINGERPRINT_TRACKER.idFromName(targetFingerprint);
    const fpTrackerStub = env.FINGERPRINT_TRACKER.get(fpTrackerId);
    
    // FP DOの /get-state エンドポイントを呼び出す
    const res = await fpTrackerStub.fetch(new Request("https://internal/get-state", {
        headers: {"X-Fingerprint-ID": targetFingerprint} // 必要であればDOにIDを渡す
    }));

    if (res.ok) {
        return new Response(await res.json(), { headers: { 'Content-Type': 'application/json' } });
    } else {
        return new Response(`Failed to get FP state: ${res.status} ${await res.text()}`, { status: res.status });
    }
  }


  // --- 1. Cookieホワイトリスト（最優先） ---
  const cookieHeader = request.headers.get("Cookie") || "";
  if (cookieHeader.includes("secret-pass=Rocaniru-Admin-Bypass-XYZ789")) {
    console.log(`[WHITELIST] Access granted via secret cookie for IP=${ip} FP=${fingerprint}.`);
    return fetch(request);
  }

  // --- 2. KVブロックリストチェック (IPまたはフィンガープリントでブロックされているか) ---
  // まずIPでチェック
  const ipStatus = await env.BOT_BLOCKER_KV.get(ip, { cacheTtl: 300 });
  if (["permanent-block", "temp-1", "temp-2", "temp-3"].includes(ipStatus)) {
    console.log(`[KV BLOCK] IP=${ip} status=${ipStatus}`);
    return new Response("Not Found", { status: 404 });
  }

  // 次にフィンガープリントでチェック
  const fpStatus = await env.BOT_BLOCKER_KV.get(`FP-${fingerprint}`, { cacheTtl: 300 }); // KVキーに "FP-" プレフィックス
  if (["permanent-block", "temp-1", "temp-2", "temp-3"].includes(fpStatus)) {
    console.log(`[KV BLOCK] FP=${fingerprint} status=${fpStatus}`);
    return new Response("Not Found", { status: 404 });
  }


  // --- 3. 静的ルールによるパス探索型攻撃ブロック ---
  if (path.includes("/wp-") || path.endsWith(".php") || path.includes("/phpmyadmin") ||
      path.endsWith("/.env") || path.endsWith("/config") || path.includes("/admin/") ||
      path.includes("/dbadmin")) {
    // ここもIPとフィンガープリントの両方をログ・ブロック処理に渡す
    return logAndBlock(ip, ua, "path-scan", env, ctx, fingerprint);
  }

  // ★★★ 変更開始: UAベースの分類と有害Bot検知の順序を調整 (SafeBotもここで判定) ★★★
  // PetalBotはsafeBotPatternsに属するため、botPatternの定義の前に移動
  const safeBotPatterns = ["PetalBot"]; // PetalBotは安全だがレート制限対象

  // AhrefsBot, PetalBot, Bingbotなどの公式ボットのUser-Agentをパターンに追加
  const botPattern = /\b(bot|crawl|spider|slurp|fetch|headless|preview|externalagent|barkrowler|bingbot|petalbot|ahrefsbot|mj12bot|crawler|scanner)\b/i; 

  let label = "[H]"; // デフォルトは人間
  let refinedLabel = "[H]"; // 最終的なラベル

  // FPTrackerのインスタンスもここで取得 (B判定でも必要になるため)
  const ipTrackerId = env.IP_STATE_TRACKER.idFromName(ip);
  const ipTrackerStub = env.IP_STATE_TRACKER.get(ipTrackerId);
  const fpTrackerId = env.FINGERPRINT_TRACKER.idFromName(fingerprint);
  const fpTrackerStub = env.FINGERPRINT_TRACKER.get(fpTrackerId);


  // まずは明確なボットかセーフボットかを判定
  if (botPattern.test(ua)) {
      label = "[B]"; // まずはボットと仮判定
      refinedLabel = "[B]";

      // セーフボットの中に含まれるかチェック
      for (const safeBotPattern of safeBotPatterns) {
          if (ua.includes(safeBotPattern)) {
              // セーフボットはレート制限対象
              const res = await ipTrackerStub.fetch(new Request("https://internal/rate-limit", {
                  headers: {"CF-Connecting-IP": ip}
              }));
              if (res.ok) {
                  const { allowed } = await res.json();
                  if (!allowed) {
                      console.log(`[RATE LIMIT] SafeBot (${safeBotPattern}) IP=${ip} blocked. (FP=${fingerprint})`);
                      return new Response("Too Many Requests", { status: 429 });
                  }
              }
              refinedLabel = "[SAFE_BOT]"; // 新しいラベルを導入して区別
              break; 
          }
      }
  }


  if (refinedLabel === "[H]") { // UAで人間と仮判定された場合のみTH/SH判定
    const fpStateRes = await fpTrackerStub.fetch(new Request("https://internal/get-state", {
        headers: {"X-Fingerprint-ID": fingerprint}
    }));

    if (fpStateRes.ok) {
        const fpState = await fpStateRes.json();
        if (fpState.jsExecuted) {
            refinedLabel = "[TH]"; // 本物の人間 (Trusted Human)
        } else {
            refinedLabel = "[SH]"; // 疑わしい人間 (Suspicious Human)
        }
    } else {
        console.error(`[DO_ERROR] Failed to get FP state for ${fingerprint}. Status: ${fpStateRes.status}. Treating as SH.`);
        refinedLabel = "[SH]"; // 安全のためSHとして扱う
    }
  }
  
  // ★★★ 最終的なラベルを出力する場所をここに集約 ★★★
  // SAFE_BOTもここでログに出力される
  console.log(`${refinedLabel} ${request.url} IP=${ip} UA=${ua} FP=${fingerprint}`);

  // THまたはSAFE_BOTであれば、ここで処理を終了し、修正済みレスポンスを返す (パフォーマンス最適化)
  if (refinedLabel === "[TH]" || refinedLabel === "[SAFE_BOT]") { 
    return fetch(request);
  }

  // --- 有害Bot検知＋ペナルティ (ラベルがBの場合の処理) ---
  if (refinedLabel === "[B]") { // UAでボットと判定された場合（SAFE_BOTはここで処理されない）
    if (learnedBadBotsCache === null) {
      const learnedList = await env.BOT_BLOCKER_KV.get("LEARNED_BAD_BOTS", { type: "json" });
      learnedBadBotsCache = new Set(Array.isArray(learnedList) ? Array.from(learnedList) : []); // Array.fromを追加してSetをコピー
    }
    for (const patt of learnedBadBotsCache) {
      if (new RegExp(patt, "i").test(ua)) {
        const reason = `unwanted-bot(learned):${patt}`;
        const ipRes = await ipTrackerStub.fetch(new Request("https://internal/trigger-violation", {
          headers: {"CF-Connecting-IP": ip}
        }));
        const fpRes = await fpTrackerStub.fetch(new Request("https://internal/track-violation", {
          headers: {"X-Fingerprint-ID": fingerprint}
        }));

        if (ipRes.ok && fpRes.ok) {
          const { count: ipCount } = await ipRes.json();
          const { count: fpCount } = await fpRes.json();
          await handleViolationSideEffects(ip, ua, reason, ipCount, env, ctx, fingerprint, fpCount);
        } else {
            console.error(`[DO_ERROR] Failed to trigger violation for IP=${ip} FP=${fingerprint}. IP DO Status: ${ipRes.status}, FP DO Status: ${fpRes.status}`);
        }
        return new Response("Not Found", { status: 404 });
      }
    }
    if (badBotDictionaryCache === null) {
      const object = await env.BLOCKLIST_R2.get("dictionaries/bad-bots.txt");
      badBotDictionaryCache = object
        ? (await object.text()).split('\n').filter(line => line && !line.startsWith('#'))
        : [];
    }
    for (const patt of badBotDictionaryCache) {
      if (new RegExp(patt, "i").test(ua)) {
        const reason = `unwanted-bot(new):${patt}`;
        console.log(`[LEARNED] New bad bot pattern: ${patt}`);
        learnedBadBotsCache.add(patt);
        ctx.waitUntil(env.BOT_BLOCKER_KV.put("LEARNED_BAD_BOTS", JSON.stringify(Array.from(learnedBadBotsCache))));
        
        const ipRes = await ipTrackerStub.fetch(new Request("https://internal/trigger-violation", {
          headers: {"CF-Connecting-IP": ip}
        }));
        const fpRes = await fpTrackerStub.fetch(new Request("https://internal/track-violation", {
          headers: {"X-Fingerprint-ID": fingerprint}
        }));

        if (ipRes.ok && fpRes.ok) {
          const { count: ipCount } = await ipRes.json();
          const { count: fpCount } = await fpRes.json();
          await handleViolationSideEffects(ip, ua, reason, ipCount, env, ctx, fingerprint, fpCount);
        } else {
            console.error(`[DO_ERROR] Failed to trigger violation for IP=${ip} FP=${fingerprint}. IP DO Status: ${ipRes.status}, FP DO Status: ${fpRes.status}`);
        }
        return new Response("Not Found", { status: 404 });
      }
    }
  }

  // ★★★ 変更終了 ★★★


  // --- 4. アセットファイルならそのまま返す（JSピクセル検出は残す） ---
  const EXT_SKIP = /\.(jpg|jpeg|png|gif|svg|webp|js|css|woff2?|ttf|ico|map|txt|eot|otf|json|xml|avif)(\?|$)/; // JSファイルもスキップ対象に含める
  // Shopify MonorailのようなJSピクセルもここに含まれる
  if (EXT_SKIP.test(path)) {
    const monorailPixelPattern = /^\/\.well-known\/shopify\/monorail\//;
    const importantJsPatterns = [ // importantJsPatternsはここで定義（アセットスキップの後）
      /^\/\.well-known\/shopify\/monorail\//, // Monorail V1
      /^\/\.well-known\/shopify\/monorail\/unstable\/produce_batch/, // Monorail V2
      /^\/cdn\/shopifycloud\/portable-wallets\/latest\/accelerated-checkout-backwards-compat\.css/, // CSSだがJSと関連することもある
      /^\/cdn\/shopifycloud\/privacy-banner\/storefront-banner\.js/, // プライバシーバナーJS
      /^\/cart\.js/, // cart.js
      /^\/cdn\/shop\/t\/\d+\/assets\/theme\.min\.js(\?|$)/, // 例: theme.min.js
      /^\/cdn\/shop\/t\/\d+\/assets\/global\.js(\?|$)/, // 例: global.js
      /^\/cdn\/shopify\/s\/files\/.*\.js(\?|$)/, // ShopifyアプリのJSなど
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
      const doRes = await fpTrackerStub.fetch(new Request("https://internal/record-js-execution", {
          method: 'POST',
          headers: { "X-Fingerprint-ID": fingerprint }
      }));

      if (!doRes.ok) {
          console.error(`[DO_ERROR] Failed to record JS execution for FP=${fingerprint} via Monorail/Important JS. Status: ${doRes.status}`);
      } else {
          // console.log(`[JS_IMPORTANT_DETECTED] FP=${fingerprint} detected important JS: ${path}`); // デバッグ用、コメントアウト
      }
    }
    return fetch(request);
  }

  // --- 6. 動的ルール実行（Bot／Human別） ---
  // Humanアクセス（THまたはSH）に対する動的ルール実行
  // THは原則スキップされるので、SHの場合にのみ実行される
  if (refinedLabel === "[SH]") {
    // ロケールチェックもIPとフィンガープリントの両方で実施する
    const ipLocaleRes = await ipTrackerStub.fetch(new Request("https://internal/check-locale", {
      method: 'POST',
      headers: {
        "CF-Connecting-IP": ip,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ path })
    }));

    const fpLocaleRes = await fpTrackerStub.fetch(new Request("https://internal/check-locale-fp", {
      method: 'POST',
      headers: {
        "X-Fingerprint-ID": fingerprint, // FP用のDOにFPを渡す
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ path })
    }));

    ctx.waitUntil(fpTrackerStub.fetch(new Request("https://internal/track-behavior", {
      method: 'POST',
      headers: {
        "X-Fingerprint-ID": fingerprint,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ path: url.pathname }) // アクセスパスを送信
    }));


    // どちらかのロケールチェックで違反が検知されたらブロック
    let violationDetected = false;
    let ipCount = 0;
    let fpCount = 0;
    let reason = "locale-fanout"; // デフォルトの理由

    if (ipLocaleRes.ok) {
      const { violation, count } = await ipLocaleRes.json();
      if (violation) {
        violationDetected = true;
        ipCount = count;
      }
    } else {
      console.error(`[DO_ERROR] IP DO /check-locale failed for IP=${ip}. Status: ${ipLocaleRes.status}`);
    }

    if (fpLocaleRes.ok) {
      const { violation, count } = await fpLocaleRes.json();
      if (violation) {
        violationDetected = true;
        fpCount = count;
      }
    } else {
      console.error(`[DO_ERROR] FP DO /check-locale-fp failed for FP=${fingerprint}. Status: ${fpLocaleRes.status}`);
    }

    if (violationDetected) {
      await handleViolationSideEffects(ip, ua, reason, Math.max(ipCount, fpCount), env, ctx, fingerprint, fpCount);
      return new Response("Not Found", { status: 404 });
    }
  }


  // Amazon Botなりすましチェック
  // これはIPベースのままにするか、より高度な方法でFPも考慮するか検討
  if (ua.startsWith("AmazonProductDiscovery/1.0")) {
    const isVerified = await verifyBotIp(ip, "amazon", env); // IPベース
    if (!isVerified) {
      const reason = "amazon-impersonation";
      // IPベースのカウントを更新
      const ipRes = await ipTrackerStub.fetch(new Request("https://internal/trigger-violation", {
        headers: {"CF-Connecting-IP": ip}
      }));
      // フィンガープリントベースのカウントを更新
      const fpRes = await fpTrackerStub.fetch(new Request("https://internal/track-violation", {
        headers: {"X-Fingerprint-ID": fingerprint}
      }));

      if (ipRes.ok && fpRes.ok) {
        const { count: ipCount } = await ipRes.json();
        const { count: fpCount } = await fpRes.json();
        await handleViolationSideEffects(ip, ua, reason, Math.max(ipCount, fpCount), env, ctx, fingerprint, fpCount);
      } else {
        console.error(`[DO_ERROR] Failed to trigger violation for IP=${ip} FP=${fingerprint}. IP DO Status: ${ipRes.status}, FP DO Status: ${fpRes.status}`);
      }
      return new Response("Not Found", { status: 404 });
    }
  }

  // --- 7. 全チェッククリア → 正常アクセス処理へ ---
  return fetch(request);
}


// --- 3. コアヘルパー関数 ---

// ★変更: fingerprint と fpCount パラメータを追加★
async function handleViolationSideEffects(ip, ua, reason, ipCount, env, ctx, fingerprint, fpCount) {
  // ログ出力もIPとFPの両方を表示するように変更
  console.log(`[VIOLATION] IP=${ip} FP=${fingerprint} reason=${reason} IP_count=${ipCount} FP_count=${fpCount}`);

  // ブロック判断はIPベースのカウントとFPベースのカウントのどちらか高い方、または両方の複合で判断することも検討
  // ここでは、どちらかのDOから返されたカウント（Math.maxで取得）を `effectiveCount` として利用
  const effectiveCount = Math.max(ipCount, fpCount);

  // KVへの書き込みはIPとFPの両方に対して行う
  // expirationTtlは、個々のIP/FPの特性に合わせて調整可能
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
    // 永久ブロック
    ctx.waitUntil(env.BOT_BLOCKER_KV.put(ip, "permanent-block"));
    ctx.waitUntil(env.BOT_BLOCKER_KV.put(`FP-${fingerprint}`, "permanent-block"));

    // R2へのログ記録もIPとFPの両方を含める
    const record = JSON.stringify({ 
      ip, 
      fingerprint, // FPを追加
      userAgent: ua, 
      reason, 
      ipCount,    // IPの最終カウント
      fpCount,    // FPの最終カウント
      timestamp: new Date().toISOString() 
    });
    // R2のオブジェクト名もIPとFPを組み合わせるなどして一意性を高める
    ctx.waitUntil(env.BLOCKLIST_R2.put(`${ip}-${fingerprint.substring(0, 8)}-${Date.now()}.json`, record));
    // KVに高カウントFPとして登録
    ctx.waitUntil(env.BOT_BLOCKER_KV.put(`FP-HIGH-COUNT-${fingerprint}`, "pending-permanent-block", { expirationTtl: 3600 * 24 }));
  }
}

// logAndBlock 関数もFPを受け取るように修正
function logAndBlock(ip, ua, reason, env, ctx, fingerprint) {
  console.log(`[STATIC BLOCK] IP=${ip} FP=${fingerprint} reason=${reason} UA=${ua}`);
  // ここで、このFPもブロック対象としたいならKVに書き込む処理を追加することも可能
  // ctx.waitUntil(env.BOT_BLOCKER_KV.put(`FP-${fingerprint}`, "static-block", { expirationTtl: 3600 })); // 例: 1時間ブロック
  return new Response("Not Found", { status: 404 });
}


async function verifyBotIp(ip, botKey, env) {
  if (botCidrsCache === null) {
    botCidrsCache = await env.BOT_BLOCKER_KV.get("BOT_CIDRS", { type: "json", cacheTtl: 3600 });
  }
  const cidrs = botCidrsCache ? botCidrsCache[botKey] : null;
  if (!cidrs || !Array.isArray(cidrs) || cidrs.length === 0) {
    console.warn(`CIDR list for bot '${botKey}' is empty or not found in KV.`);
    return false;
  }
  return cidrs.some(cidr => ipInCidr(ip, cidr));
}


// --- 4. ユーティリティ関数 ---

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

function ipInCidr(ip, cidr) {
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
    console.error(`[ipInCidr] Error: ip='${ip}' cidr='${cidr}'`, e);
    return false;
  }
}
