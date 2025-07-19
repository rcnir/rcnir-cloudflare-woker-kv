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
export { IPStateTracker };

let botCidrsCache = null;
let unwantedBotPatternsCache = null;

export default {
  async fetch(request, env, ctx) {
    return handle(request, env, ctx);
  },

  async scheduled(event, env, ctx) {
    console.log("Cron Trigger fired: Syncing permanent block list...");
    const id = env.IP_STATE_TRACKER.idFromName("sync-job"); // Binding名を変更
    const stub = env.IP_STATE_TRACKER.get(id);
    const res = await stub.fetch("https://internal/list-high-count");
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
  }
};


// --- 2. メインロジック ---

async function handle(request, env, ctx) {
  const ua = request.headers.get("User-Agent") || "UA_NOT_FOUND";
  const ip = request.headers.get("CF-Connecting-IP") || "IP_NOT_FOUND";
  const { pathname } = new URL(request.url);
  const path = pathname.toLowerCase();

  const status = await env.BOT_BLOCKER_KV.get(ip, { cacheTtl: 300 });
  if (status === "permanent-block" || status === "temp-1" || status === "temp-2" || status === "temp-3") {
    console.log(`[KV BLOCK] IP=${ip} status=${status}`);
    return new Response("Not Found", { status: 404 });
  }

  const staticBlockIps = new Set([]);
  for (const block of staticBlockIps) {
    if (ipInCidr(ip, block)) {
      return logAndBlock(ip, ua, "static-ip", env, ctx);
    }
  }
  if (path.includes("/wp-") || path.endsWith(".php") || path.includes("/phpmyadmin") ||
      path.endsWith("/.env") || path.endsWith("/config") || path.includes("/admin/") ||
      path.includes("/dbadmin")) {
    return logAndBlock(ip, ua, "path-scan", env, ctx);
  }

  const EXT_SKIP = /\.(jpg|jpeg|png|gif|svg|webp|js|css|woff2?|ttf|ico|map|txt|eot|otf|json|xml|avif)(\?|$)/;
  const PATH_SKIP = path.startsWith("/wpm@") || path.includes("/cart.js") ||
                    path.includes("/recommendations/") || path.startsWith("/_t/");
  if (EXT_SKIP.test(path) || PATH_SKIP) {
    return fetch(request);
  }

  const botPattern = /(bot|crawl|spider|slurp|fetch|headless|preview|externalagent|barkrowler|bingbot|petalbot)/i;
  const label = botPattern.test(ua) ? "[B]" : "[H]";
  console.log(`${label} ${request.url} IP=${ip} UA=${ua}`);

  const id = env.IP_STATE_TRACKER.idFromName(ip);
  const stub = env.IP_STATE_TRACKER.get(id);

  if (label === "[B]") {
    // ステップ1：学習済みリスト（KV）に載っているか確認
    if (learnedBadBotsCache === null) {
      const learnedList = await env.BOT_BLOCKER_KV.get("LEARNED_BAD_BOTS", { type: "json" });
      learnedBadBotsCache = new Set(Array.isArray(learnedList) ? learnedList : []);
    }
    for (const patt of learnedBadBotsCache) {
      if (new RegExp(patt, "i").test(ua)) {
        // 学習済みのボットなので即ブロック
        const reason = `unwanted-bot(learned):${patt}`;
        const res = await stub.fetch(new Request("https://internal/trigger-violation", { headers: { "CF-Connecting-IP": ip } }));
        if (res.ok) {
          const { count } = await res.json();
          await handleViolationSideEffects(ip, ua, reason, count, env, ctx);
        }
        return new Response("Not Found", { status: 404 });
      }
    }

    // ステップ2：辞書（R2）と照合して、新しい有害ボットか判断
    if (badBotDictionaryCache === null) {
        const object = await env.BLOCKLIST_R2.get("dictionaries/bad-bots.txt");
        if (object !== null) {
            const dictionaryText = await object.text();
            badBotDictionaryCache = dictionaryText.split('\n').filter(line => line && !line.startsWith('#'));
        } else {
            badBotDictionaryCache = [];
        }
    }
    for (const patt of badBotDictionaryCache) {
        if (new RegExp(patt, "i").test(ua)) {
            const reason = `unwanted-bot(new):${patt}`;
            console.log(`[LEARNED] New bad bot pattern: ${patt}`);
            
            learnedBadBotsCache.add(patt);
            ctx.waitUntil(env.BOT_BLOCKER_KV.put("LEARNED_BAD_BOTS", JSON.stringify(Array.from(learnedBadBotsCache))));

            const res = await stub.fetch(new Request("https://internal/trigger-violation", { headers: { "CF-Connecting-IP": ip } }));
            if (res.ok) {
                const { count } = await res.json();
                await handleViolationSideEffects(ip, ua, reason, count, env, ctx);
            }
            return new Response("Not Found", { status: 404 });
        }
    }
  }

  if (label === "[H]") {
    const locale = extractLocale(path);
    const res = await stub.fetch(new Request("https://internal/check-locale", {
        method: 'POST',
        headers: { "CF-Connecting-IP": ip, "Content-Type": "application/json" },
        body: JSON.stringify({ locale })
    }));
    if (res.ok) {
        const { violation, count } = await res.json();
        if (violation) {
            await handleViolationSideEffects(ip, ua, "locale-fanout", count, env, ctx);
            return new Response("Not Found", { status: 404 });
        }
    } else {
        console.error(`DO /check-locale failed for IP=${ip}. Status: ${res.status}`);
    }
  }

  if (ua.startsWith("AmazonProductDiscovery/1.0")) {
    const isVerified = await verifyBotIp(ip, "amazon", env);
    if (!isVerified) {
      const reason = "amazon-impersonation";
      const res = await stub.fetch(new Request("https://internal/trigger-violation", { headers: { "CF-Connecting-IP": ip } }));
      if(res.ok) {
        const { count } = await res.json();
        await handleViolationSideEffects(ip, ua, reason, count, env, ctx);
      }
      return new Response("Not Found", { status: 404 });
    }
  }
  
  return fetch(request);
}


// --- 3. コアヘルパー関数 ---

async function handleViolationSideEffects(ip, ua, reason, count, env, ctx) {
  console.log(`[VIOLATION] IP=${ip} reason=${reason} count=${count}`);

  if (count === 1) {
    // 1回目：10分ブロック
    ctx.waitUntil(env.BOT_BLOCKER_KV.put(ip, "temp-1", { expirationTtl: 600 }));
  } else if (count === 2) {
    // 2回目：10分ブロック
    ctx.waitUntil(env.BOT_BLOCKER_KV.put(ip, "temp-2", { expirationTtl: 600 }));
  } else if (count === 3) {
    // 3回目：24時間ブロック
    const twentyFourHours = 24 * 3600;
    ctx.waitUntil(env.BOT_BLOCKER_KV.put(ip, "temp-3", { expirationTtl: twentyFourHours }));
  } else if (count >= 4) {
    // 4回目：永久ブロック
    ctx.waitUntil(env.BOT_BLOCKER_KV.put(ip, "permanent-block"));
    const record = JSON.stringify({ ip, userAgent: ua, reason, count, timestamp: new Date().toISOString() });
    ctx.waitUntil(env.BLOCKLIST_R2.put(`${ip}-${Date.now()}.json`, record));
  }
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

function logAndBlock(ip, ua, reason, env, ctx) {
  console.log(`[STATIC BLOCK] IP=${ip} reason=${reason} UA=${ua}`);
  return new Response("Not Found", { status: 404 });
}


// --- 4. ユーティリティ関数 ---

function extractLocale(path) {
  const seg = path.split('/').filter(Boolean)[0];
  if (!seg) return 'root';
  if (/^[a-z]{2}(-[a-z]{2})?$/.test(seg)) return seg;
  return 'root';
}

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
