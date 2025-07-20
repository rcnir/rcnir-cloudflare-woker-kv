// src/do/FingerprintTracker.js

// Durable Object クラスの定義 (状態管理用)
export class FingerprintTracker {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    // 必要に応じて、DO内で共有する設定などを初期化
    // 例: this.VIOLATION_THRESHOLD = 5;
  }

  async fetch(request) {
    const url = new URL(request.url);
    const fpId = request.headers.get("X-Fingerprint-ID"); // Workerから渡されるフィンガープリントID

    if (!fpId) {
      return new Response("Missing X-Fingerprint-ID header", { status: 400 });
    }

    switch (url.pathname) {
      case "/track-violation": {
        // このフィンガープリントIDの違反カウントを管理・更新
        let state = await this.state.storage.get("violations") || { count: 0 };
        state.count += 1;
        await this.state.storage.put("violations", state);
        console.log(`[FP_VIOLATION] FP_ID=${fpId} count=${state.count}`); // デバッグログ
        return new Response(JSON.stringify({ count: state.count }), { status: 200 });
      }
      case "/get-state": {
        // 特定のフィンガープリントIDの状態を取得するデバッグ用エンドポイント
        const state = await this.state.storage.get("violations") || { count: 0 };
        return new Response(JSON.stringify(state), { status: 200 });
      }
      case "/reset-state": {
        // フィンガープリントIDのデータをリセットするエンドポイント
        // ★★★ 認証を必ず追加すること！ ★★★
        const resetKey = url.searchParams.get("reset_key");
        if (!this.env.DO_RESET_KEY || resetKey !== this.env.DO_RESET_KEY) {
          return new Response("Unauthorized reset attempt.", { status: 401 });
        }
        await this.state.storage.deleteAll();
        console.log(`[FP_RESET] Fingerprint ID ${fpId} data reset.`);
        return new Response("Fingerprint data reset successfully.", { status: 200 });
      }
      default:
        return new Response("Not found", { status: 404 });
    }
  }
}

// フィンガープリントを生成する関数 (Durable Object クラスの外に定義)
export async function generateFingerprint(request) {
  const headers = request.headers;
  const cf = request.cf; // request.cf オブジェクト (Cloudflareが自動的に付与)

  let fingerprintString = "";

  // 1. User-Agent (必須)
  fingerprintString += headers.get("User-Agent") || "";

  // 2. Accept ヘッダー群
  fingerprintString += headers.get("Accept-Language") || "";
  fingerprintString += headers.get("Accept-Encoding") || "";
  fingerprintString += headers.get("Accept") || "";

  // 3. Client Hints (存在すれば)
  // ブラウザによっては送信されないが、あれば含める
  fingerprintString += headers.get("Sec-Ch-Ua") || "";
  fingerprintString += headers.get("Sec-Ch-Ua-Mobile") || "";
  fingerprintString += headers.get("Sec-Ch-Ua-Platform") || "";

  // 4. Sec-Fetch ヘッダー群 (ブラウザが自動付与)
  fingerprintString += headers.get("Sec-Fetch-Site") || "";
  fingerprintString += headers.get("Sec-Fetch-Mode") || "";
  fingerprintString += headers.get("Sec-Fetch-Dest") || "";
  fingerprintString += headers.get("Sec-Fetch-User") || "";

  // 5. Referer (ある場合)
  // Refererはプライバシー問題や欠落の可能性もあるが、ボット識別には役立つ
  fingerprintString += headers.get("Referer") || "";

  // 6. Cloudflare メタデータ (request.cf)
  // ASNや国コードはボットが利用するデータセンターを特定するヒントになる
  fingerprintString += cf.asn || "";       // AS番号 (例: 13335 for Cloudflare)
  fingerprintString += cf.country || "";   // 国コード (例: JP, US)
  fingerprintString += cf.city || "";      // 市町村 (精度はIPによる)
  fingerprintString += cf.region || "";    // 地域コード

  // 7. IPアドレスのサブネットの一部 (IPが変わっても同じネットワークからの可能性)
  // IPv4の最初の3オクテット、IPv6の最初の4セグメント
  const ip = headers.get("CF-Connecting-IP");
  if (ip) {
    if (ip.includes('.')) { // IPv4
      fingerprintString += ip.split('.').slice(0, 3).join('.');
    } else if (ip.includes(':')) { // IPv6
      fingerprintString += ip.split(':').slice(0, 4).join(':');
    }
  }

  // --- ハッシュ化 ---
  // 連結した文字列をSHA-256でハッシュ化してフィンガープリントIDを生成
  // これが、特定のユーザー/ボットの「デジタル指紋」となります
  const encoder = new TextEncoder();
  const data = encoder.encode(fingerprintString);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  const fingerprint = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');

  return fingerprint;
}
