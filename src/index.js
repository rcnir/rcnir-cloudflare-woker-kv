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
 *
 * =================================================================
 */

// --- 1. エクスポートとメインハンドラ ---

import { IPStateTracker } from "./do/IPStateTracker.js";
// ★追加: FingerprintTracker DOとフィンガープリント生成関数をインポート★
import { FingerprintTracker, generateFingerprint } from "./do/FingerprintTracker.js";

export { IPStateTracker };
export { FingerprintTracker }; // ★追加: Durable Objectとして公開するために必要★

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
      return;
    }
    const ipsToBlock = await res.json();
    if (ipsToBlock && ipsToBlock.length > 0) {
      const promises = ipsToBlock.map(ip => env.BOT_BLOCKER_KV.put(ip, "permanent-block"));
      await Promise.all(promises);
      console.log(`Synced ${ipsToBlock.length} permanent block IPs to KV.`);
    } else {
      console.log("No new IPs to permanently block.");
    }

    // ★追加: FingerprintTrackerから高カウントフィンガープリントを取得し、KVに同期★
    const fpSyncId = env.FINGERPRINT_TRACKER.idFromName("sync-job-fp");
    const fpStub = env.FINGERPRINT_TRACKER.get(fpSyncId);
    const fpRes = await fpStub.fetch(new Request("https://internal/list-high-count-fp")); // FingerprintTrackerから高カウントFPを取得
    if (!fpRes.ok) {
      console.error(`Failed to fetch high count Fingerprints from DO. Status: ${fpRes.status}`);
      return;
    }
    const fpsToBlock = await fpRes.json();
    if (fpsToBlock && fpsToBlock.length > 0) {
      const promises = fpsToBlock.map(fp => env.BOT_BLOCKER_KV.put(`FP-${fp}`, "permanent-block")); // KVキーに "FP-" プレフィックス
      await Promise.all(promises);
      console.log(`Synced ${fpsToBlock.length} permanent block Fingerprints to KV.`);
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

  // ★追加: リクエストからフィンガープリントを生成★
  const fingerprint = await generateFingerprint(request);
  // console.log(`[DEBUG] FP=${fingerprint} IP=${ip}`); // デバッグ用にフィンガープリントを出力

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

  // --- 4. アセットファイルならそのまま ---
  const EXT_SKIP = /\.(jpg|jpeg|png|gif|svg|webp|js|css|woff2?|ttf|ico|map|txt|eot|otf|json|xml|avif)(\?|$)/;
  if (EXT_SKIP.test(path)) {
    return fetch(request);
  }

  // --- 5. UAベースの分類と、安全Botのレート制御 ---
  const botPattern = /(bot|crawl|spider|slurp|fetch|headless|preview|externalagent|barkrowler|bingbot|petalbot)/i;
  const label = botPattern.test(ua) ? "[B]" : "[H]";
  console.log(`${label} ${request.url} IP=${ip} UA=${ua} FP=${fingerprint}`); // FPログ追加

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
      return fetch(request);
    }
  }

  // --- 6. 動的ルール実行（Bot／Human別） ---
  // Durable Object の参照をIPベースとフィンガープリントベースの両方に拡張
  const ipTrackerId = env.IP_STATE_TRACKER.idFromName(ip);
  const ipTrackerStub = env.IP_STATE_TRACKER.get(ipTrackerId);

  const fpTrackerId = env.FINGERPRINT_TRACKER.idFromName(fingerprint); // ★追加: フィンガープリントベースのDO
  const fpTrackerStub = env.FINGERPRINT_TRACKER.get(fpTrackerId); // ★追加★


  // 有害Bot検知＋ペナルティ
  if (label === "[B]") {
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
        // フィンガープリントベースのカウントを更新 ★追加★
        const fpRes = await fpTrackerStub.fetch(new Request("https://internal/track-violation", {
          headers: {"X-Fingerprint-ID": fingerprint} // FP_TRACKERにフィンガープリントIDを渡す
        }));

        if (ipRes.ok && fpRes.ok) { // 両方のDO更新が成功したら
          const { count: ipCount } = await ipRes.json();
          const { count: fpCount } = await fpRes.json(); // FPのカウントも取得

          // ブロック処理はIPまたはFPの高い方で判断することもできるが、
          // まずはIPベースのまま handleViolationSideEffects を呼び出し
          // handleViolationSideEffects内でFPベースの処理も追加
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
        // フィンガープリントベースのカウントを更新 ★追加★
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

  // Humanアクセス：国跨ぎ言語切替による不正検知
  if (label === "[H]") {
    // ロケールチェックもIPとフィンガープリントの両方で実施する
    const ipLocaleRes = await ipTrackerStub.fetch(new Request("https://internal/check-locale", {
      method: 'POST',
      headers: {
        "CF-Connecting-IP": ip,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ path })
    }));

    const fpLocaleRes = await fpTrackerStub.fetch(new Request("https://internal/check-locale-fp", { // ★追加: FP用のロケールチェック
      method: 'POST',
      headers: {
        "X-Fingerprint-ID": fingerprint, // FP用のDOにFPを渡す
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ path })
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
        // console.log(`[DEBUG] IP Locale Violation: ${ip} count=${count}`); // デバッグログ
      }
    } else {
      console.error(`[DO_ERROR] IP DO /check-locale failed for IP=${ip}. Status: ${ipLocaleRes.status}`);
    }

    if (fpLocaleRes.ok) {
      const { violation, count } = await fpLocaleRes.json();
      if (violation) {
        violationDetected = true;
        fpCount = count;
        // console.log(`[DEBUG] FP Locale Violation: ${fingerprint} count=${count}`); // デバッグログ
      }
    } else {
      console.error(`[DO_ERROR] FP DO /check-locale-fp failed for FP=${fingerprint}. Status: ${fpLocaleRes.status}`);
    }

    if (violationDetected) {
      // 違反時の処理は、IPまたはFPのいずれかで違反が検知されたら実行
      // count は、違反を検知した方（IPまたはFP）のカウントを渡すか、両方を渡して判断
      // ここでは、どちらかのDOが違反と判断したら、そのDOのカウントを優先してhandleViolationSideEffectsに渡す
      // より厳しくするなら Math.max(ipCount, fpCount) などのロジックも検討
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
      // フィンガープリントベースのカウントを更新 ★追加★
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

// ★修正: fingerprint と fpCount パラメータを追加★
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
      ipCount,   // IPの最終カウント
      fpCount,   // FPの最終カウント
      timestamp: new Date().toISOString() 
    });
    // R2のオブジェクト名もIPとFPを組み合わせるなどして一意性を高める
    ctx.waitUntil(env.BLOCKLIST_R2.put(`${ip}-${fingerprint.substring(0, 8)}-${Date.now()}.json`, record));
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

// この関数はIPStateTracker.js にも存在するため、重複に注意。
// もし両方で必要なら、共有のutils.jsファイルに移動するのがベストプラクティス。
// 今回はFingerprintTracker.js に parseLocale を含めたため、ここでは削除または使わない
/*
function extractLocale(path) {
  const seg = path.split('/').filter(Boolean)[0];
  if (!seg) return 'root';
  if (/^[a-z]{2}(-[a-z]{2})?$/.test(seg)) return seg;
  return 'root';
}
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
