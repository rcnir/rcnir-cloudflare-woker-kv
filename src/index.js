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
 * npx wrangler tail shopify-bot-blocker | grep "\[H\]"
 *
 * B判定（ボット）のログをリアルタイム表示:
 * npx wrangler tail shopify-bot-blocker | grep "\[B\]"
 *
 * 違反が検知されたログだけを表示:
 * npx wrangler tail shopify-bot-blocker | grep "\[VIOLATION\]"
 *
 * KVの情報に基づいてブロックしたログだけを表示:
 * npx wrangler tail shopify-bot-blocker | grep "\[KV BLOCK\]"
 *
 * 特定IPのブロック状態を確認 (例: 192.0.2.1):
 * npx wrangler kv:key get --namespace-id="7da99382fc3945bd87bc65f55c9ea1fb" "192.0.2.1"
 *
 * 永続ブロックされたIPの全ログをR2で一覧表示:
 * npx wrangler r2 object list rocaniiru-log
 *１
 * =================================================================
 */


// --- 1. エクスポートとメインハンドラ ---
import { IPStateTracker } from "./do/IPStateTracker.js";
import { FingerprintTracker, generateFingerprint } from "./do/FingerprintTracker.js";

export { IPStateTracker };
export { FingerprintTracker };

let botCidrsCache = null;
let unwantedBotPatternsCache = null;
let learnedBadBotsCache = null;
let badBotDictionaryCache = null;

export default {
    async fetch(request, env, ctx) {
        return handle(request, env, ctx);
    },

    async scheduled(event, env, ctx) {
        // ... (この部分は前回の修正から変更なし) ...
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
async function handle(request, env, ctx) {
  const ua = request.headers.get("User-Agent") || "UA_NOT_FOUND";
  const ip = request.headers.get("CF-Connecting-IP") || "IP_NOT_FOUND";
  const url = new URL(request.url);
  const path = url.pathname.toLowerCase();
  const fingerprint = await generateFingerprint(request);

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
        return new Response(await res.json(), { headers: { "Content-Type": "application/json" } });
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

  // --- HTMLレスポンスに対するJS実行トラッカー挿入の新しいロジック ---
  const EXT_SKIP = /\.(jpg|jpeg|png|gif|svg|webp|js|css|woff2?|ttf|ico|map|txt|eot|otf|json|xml|avif)(\?|$)/;
  const botPattern = /(bot|crawl|spider|slurp|fetch|headless|preview|externalagent|barkrowler|bingbot|petalbot)/i; // UA分類用

  const isHtmlRequest = !EXT_SKIP.test(path) && request.headers.get("Accept")?.includes("text/html");

  // まずはオリジンからのレスポンスを取得
  let originalResponse = await fetch(request);
  let finalResponse = originalResponse; // デフォルトではオリジナルレスポンスを使用

  // HTMLリクエストかつ成功レスポンスの場合のみ、HTMLにスクリプトを挿入
  if (isHtmlRequest && originalResponse.ok && originalResponse.headers.get("Content-Type")?.includes("text/html")) {
      try {
          const originalHtml = await originalResponse.text();
          const jsTrackerScript = `
              <script>
              // Durable ObjectにJS実行を通知
              // パフォーマンスを考慮し、Monorailのように非同期でfire-and-forget
              fetch('/internal/record-js-execution-from-html', {
                  method: 'POST',
                  headers: {
                      'Content-Type': 'application/json',
                      'X-Fingerprint-ID': '${fingerprint}'
                  },
                  body: JSON.stringify({ timestamp: Date.now() })
              }).catch(e => console.warn('Failed to report JS execution from HTML:', e));
              </script>
          `;
          // </body>の直前にスクリプトを挿入
          const modifiedHtml = originalHtml.replace('</body>', `${jsTrackerScript}</body>`);
          // 新しいResponseオブジェクトを生成し、Content-Lengthを削除してCloudflareに再計算させる
          finalResponse = new Response(modifiedHtml, {
              status: originalResponse.status,
              statusText: originalResponse.statusText,
              headers: originalResponse.headers
          });
          finalResponse.headers.delete('Content-Length');
      } catch (e) {
          console.error(`[HTML_MODIFY_ERROR] Failed to modify HTML for FP=${fingerprint}:`, e);
          // HTML変更に失敗しても元のレスポンスを返す
          finalResponse = originalResponse;
      }
  }


  // --- 4. アセットファイルならそのまま返す（JSピクセル検出は残す） ---
  // Shopify MonorailのようなJSピクセルもここに含まれる
  if (EXT_SKIP.test(path)) {
    const monorailPixelPattern = /^\/\.well-known\/shopify\/monorail\//;
    if (monorailPixelPattern.test(path)) {
      const fpTrackerId = env.FINGERPRINT_TRACKER.idFromName(fingerprint);
      const fpTrackerStub = env.FINGERPRINT_TRACKER.get(fpTrackerId);
      // JSが実行されたことをDurable Objectに記録 (非同期)
      ctx.waitUntil(fpTrackerStub.fetch(new Request("https://internal/record-js-execution", {
        method: 'POST',
        headers: { "X-Fingerprint-ID": fingerprint } // FP用のDOにFPを渡す
      })));
    }
    return finalResponse; // ★変更: HTML変更後のレスポンスを返す
  }

  // --- 5. UAベースの分類と、安全Botのレート制御 ---
  const label = botPattern.test(ua) ? "[B]" : "[H]";
  // console.log(`${label} ${request.url} IP=${ip} UA=${ua} FP=${fingerprint}`); // このログは最終判定で出力するためコメントアウト

  let refinedLabel = label; // 最終的な判定ラベル (B, TH, SH)

  if (label === "[H]") { // UAで人間と判定された場合のみTH/SH判定
    const fpTrackerId = env.FINGERPRINT_TRACKER.idFromName(fingerprint); // ★変更: フィンガープリントベースのDO
    const fpTrackerStub = env.FINGERPRINT_TRACKER.get(fpTrackerId); // ★変更★

    // Durable ObjectからJS実行状態を取得
    const fpStateRes = await fpTrackerStub.fetch(new Request("https://internal/get-state", {
        headers: {"X-Fingerprint-ID": fingerprint} // FP用のDOにFPを渡す
    }));

    if (fpStateRes.ok) {
        const fpState = await fpStateRes.json();
        if (fpState.jsExecuted) {
            refinedLabel = "[TH]"; // 本物の人間 (Trusted Human)
        } else {
            refinedLabel = "[SH]"; // 疑わしい人間 (Suspicious Human)
        }
    } else {
        // DOからの状態取得に失敗した場合もSHとして扱うか、エラーログを出力
        console.error(`[DO_ERROR] Failed to get FP state for ${fingerprint}. Status: ${fpStateRes.status}. Treating as SH.`);
        refinedLabel = "[SH]"; // 安全のためSHとして扱う
    }
  }
  
  // ★★★ 最終的なラベルを出力する場所をここに集約 ★★★
  console.log(`${refinedLabel} ${request.url} IP=${ip} UA=${ua} FP=${fingerprint}`);


  // THであれば、ここで処理を終了し、修正済みレスポンスを返す (パフォーマンス最適化)
  if (refinedLabel === "[TH]") {
    return finalResponse;
  }


  const safeBotPatterns = ["PetalBot"];
  for (const safeBotPattern of safeBotPatterns) {
    if (ua.includes(safeBotPattern)) {
      // レート制限はIPとフィンガープリントの両方で管理するように拡張することも検討できるが、
      // まずはIPベースのままにする
      const id = env.IP_STATE_TRACKER.idFromName(ip);
      const stub = env.IP_STATE_TRACKER.get(id);
      const res = await stub.fetch(new Request("https://internal/rate-limit", {
        headers: {"CF-Connecting-IP": ip}
      }));
      if (res.ok) {
        const { allowed } = await res.json();
        if (!allowed) {
          console.log(`[RATE LIMIT] SafeBot (${safeBotPattern}) IP=${ip} blocked. (FP=${fingerprint})`);
          return new Response("Too Many Requests", { status: 429 });
        }
      }
      return finalResponse; // ★変更: HTML変更後のレスポンスを返す
    }
  }

  // --- 6. 動的ルール実行（Bot／Human別） ---
  // Durable Object の参照をIPベースとフィンガープリントベースの両方に拡張
  const ipTrackerId = env.IP_STATE_TRACKER.idFromName(ip);
  const ipTrackerStub = env.IP_STATE_TRACKER.get(ipTrackerId);

  const fpTrackerId = env.FINGERPRINT_TRACKER.idFromName(fingerprint); // ★変更: フィンガープリントベースのDO
  const fpTrackerStub = env.FINGERPRINT_TRACKER.get(fpTrackerId); // ★変更★


  // 有害Bot検知＋ペナルティ (ラベルは refinedLabel を使用)
  if (refinedLabel === "[B]") { // UAでボットと判定された場合
    if (learnedBadBotsCache === null) {
      const learnedList = await env.BOT_BLOCKER_KV.get("LEARNED_BAD_BOTS", { type: "json" });
      learnedBadBotsCache = new Set(Array.isArray(learnedList) ? learnedList : []);
    }
    for (const patt of learnedBadBotsCache) {
      if (new RegExp(patt, "i").test(ua)) {
        const reason = `unwanted-bot(learned):${patt}`;
        // IPベースのカウントを更新
        const ipRes = await ipTrackerStub.fetch(new Request("https://internal/trigger-violation", {
          headers: {"CF-Connecting-IP": ip}
        }));
        // フィンガープリントベースのカウントを更新
        const fpRes = await fpTrackerStub.fetch(new Request("https://internal/track-violation", {
          headers: {"X-Fingerprint-ID": fingerprint} // FP_TRACKERにフィンガープリントIDを渡す
        }));

        if (ipRes.ok && fpRes.ok) { // 両方のDO更新が成功したら
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
        
        // IPベースのカウントを更新
        const ipRes = await ipTrackerStub.fetch(new Request("https://internal/trigger-violation", {
          headers: {"CF-Connecting-IP": ip}
        }));
        // フィンガープリントベースのカウントを更新
        const fpRes = await fpTrackerStub.fetch(new Request("https://internal/track-violation", {
          headers: {"X-Fingerprint-ID": fingerprint}
        }));

        if (ipRes.ok && fpRes.ok) { // 両方のDO更新が成功したら
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

    // ★★★ 変更: 行動パターン追跡のためのDO呼び出し ★★★
    // SHの場合にのみ実行される
    ctx.waitUntil(fpTrackerStub.fetch(new Request("https://internal/track-behavior", {
      method: 'POST',
      headers: {
        "X-Fingerprint-ID": fingerprint,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ path: url.pathname }) // アクセスパスを送信
    })));
    // ★★★ 変更ここまで ★★★


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
  return finalResponse; // ★変更: HTML変更後のレスポンスを返す
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
