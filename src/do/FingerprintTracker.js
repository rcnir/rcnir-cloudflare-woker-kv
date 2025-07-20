// src/do/FingerprintTracker.js

// 設定可能な定数
const RATE_LIMIT_WINDOW_MS = 10 * 1000; // 10秒
const RATE_LIMIT_THRESHOLD = 5;       // 10秒間に5リクエスト以上で違反

const PATH_HISTORY_WINDOW_MS = 60 * 1000; // 60秒
const PATH_HISTORY_THRESHOLD = 10;      // 60秒間に10個以上の異なるパスで違反

export class FingerprintTracker {
    constructor(state, env) {
        this.state = state;
        this.env = env;
        // Durable Objectのストレージから状態を復元
        this.state.blockConcurrencyWhile(async () => {
            this.stateData = (await this.state.storage.get("state")) || {
                count: 0, // 既存の違反カウント (Durable Object全体の違反カウント)
                lastAccessTime: 0,
                requestCount: 0,
                pathHistory: [], // { path: string, timestamp: number }[]
                lastResetTime: Date.now(), // レート制限のリセット時間
                localeViolationCount: 0, // ロケールファンアウト用のカウント
                lastLocale: null, // 前回のロケール
                lastLocaleTime: 0, // 前回のロケールアクセス時刻
            };
        });
    }

    async fetch(request) {
        const url = new URL(request.url);
        const now = Date.now();

        // 状態をロード（コンストラクタで既にロードされているはずだが、念のため）
        if (!this.stateData) {
            this.stateData = await this.state.storage.get("state") || {
                count: 0,
                lastAccessTime: 0,
                requestCount: 0,
                pathHistory: [],
                lastResetTime: now,
                localeViolationCount: 0,
                lastLocale: null,
                lastLocaleTime: 0,
            };
        }

        // 内部APIエンドポイントの処理
        switch (url.pathname) {
            case "/track-violation": {
                // 既存の違反追跡ロジック
                let currentCount = this.stateData.count;
                currentCount++;
                this.stateData.count = currentCount;
                await this.state.storage.put("state", this.stateData);
                return new Response(JSON.stringify({ count: currentCount }), { headers: { 'Content-Type': 'application/json' } });
            }
            case "/check-locale-fp": {
                // ロケールチェックロジック (FingerprintTracker用)
                const { path } = await request.json(); // Workerからpathを受信

                const { lang, country } = parseLocale(path); // parseLocaleは後で定義

                // 'unknown'なロケールは処理対象外とする
                if (lang === "unknown" || country === "unknown") {
                    return new Response(JSON.stringify({ violation: false, count: this.stateData.localeViolationCount }), {
                        headers: { "Content-Type": "application/json" }
                    });
                }

                let violationDetected = false;
                // 古い記録の掃除 (LOCALE_WINDOW_MS は IPStateTracker と合わせるか、このDO内で定義)
                // ここではシンプルに、前回のロケールと国を比較
                const LOCALE_WINDOW_MS = 10 * 1000; // 10秒

                if (this.stateData.lastLocale && this.stateData.lastLocale !== `${lang}-${country}`) {
                    if (now - this.stateData.lastLocaleTime < LOCALE_WINDOW_MS) { // 10秒以内に異なるロケール
                        violationDetected = true;
                        this.stateData.localeViolationCount++; // ロケール違反カウント増加
                        this.stateData.count = Math.max(this.stateData.count, this.stateData.localeViolationCount); // 全体カウントも更新
                    }
                }
                
                this.stateData.lastLocale = `${lang}-${country}`;
                this.stateData.lastLocaleTime = now;
                
                await this.state.storage.put("state", this.stateData); // 状態を保存

                return new Response(JSON.stringify({ violation: violationDetected, count: this.stateData.localeViolationCount }), { headers: { 'Content-Type': 'application/json' } });
            }
            case "/track-behavior": {
                // ★ 新しい行動パターン追跡ロジック ★
                const { path } = await request.json();

                // 1. レート制限のチェックと更新
                if (now - this.stateData.lastResetTime > RATE_LIMIT_WINDOW_MS) {
                    this.stateData.requestCount = 0;
                    this.stateData.lastResetTime = now;
                }
                this.stateData.requestCount++;
                if (this.stateData.requestCount > RATE_LIMIT_THRESHOLD) {
                    console.log(`[FP_BEHAVIOR_VIOLATION] High request rate for FP. Count: ${this.stateData.requestCount}`);
                    this.stateData.count++; // 違反カウント増加
                }

                // 2. パス履歴の追跡と不自然な遷移のチェック
                this.stateData.pathHistory.push({ path, timestamp: now });
                // 古い履歴を削除
                this.stateData.pathHistory = this.stateData.pathHistory.filter(entry => now - entry.timestamp < PATH_HISTORY_WINDOW_MS);

                const uniquePaths = new Set(this.stateData.pathHistory.map(entry => entry.path));
                if (uniquePaths.size > PATH_HISTORY_THRESHOLD) {
                    console.log(`[FP_BEHAVIOR_VIOLATION] Too many unique paths for FP. Unique paths: ${uniquePaths.size}`);
                    this.stateData.count++; // 違反カウント増加
                }

                this.stateData.lastAccessTime = now;
                await this.state.storage.put("state", this.stateData); // 状態を保存

                return new Response(JSON.stringify({
                    count: this.stateData.count, // FP全体の違反カウント
                    requestCount: this.stateData.requestCount,
                    uniquePaths: uniquePaths.size
                }), { headers: { 'Content-Type': 'application/json' } });
            }
            case "/get-state": {
                // デバッグ用: 現在の状態を取得
                return new Response(JSON.stringify(this.stateData), { headers: { 'Content-Type': 'application/json' } });
            }
            case "/reset-state": {
                // フィンガープリントIDのデータをリセットするエンドポイント
                // ★★★ 認証を必ず追加すること！ ★★★
                const resetKey = url.searchParams.get("reset_key");
                if (!this.env.DO_RESET_KEY || resetKey !== this.env.DO_RESET_KEY) {
                  return new Response("Unauthorized reset attempt.", { status: 401 });
                }
                await this.state.storage.deleteAll();
                console.log(`[FP_RESET] Fingerprint ID data reset.`);
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
  const cf = request.cf || {}; // request.cf オブジェクト

  let fingerprintString = "";

  // 1. User-Agent (必須)
  fingerprintString += headers.get("User-Agent") || "";

  // 2. Accept ヘッダー群
  fingerprintString += headers.get("Accept-Language") || "";
  fingerprintString += headers.get("Accept-Encoding") || "";
  fingerprintString += headers.get("Accept") || "";

  // 3. Client Hints (存在すれば)
  fingerprintString += headers.get("Sec-Ch-Ua") || "";
  fingerprintString += headers.get("Sec-Ch-Ua-Mobile") || "";
  fingerprintString += headers.get("Sec-Ch-Ua-Platform") || "";

  // 4. Sec-Fetch ヘッダー群 (ブラウザが自動付与)
  fingerprintString += headers.get("Sec-Fetch-Site") || "";
  fingerprintString += headers.get("Sec-Fetch-Mode") || "";
  fingerprintString += headers.get("Sec-Fetch-Dest") || "";
  fingerprintString += headers.get("Sec-Fetch-User") || "";

  // 5. Referer (ある場合)
  fingerprintString += headers.get("Referer") || "";

  // 6. Cloudflare メタデータ (request.cf)
  fingerprintString += cf.asn || "";       // AS番号
  fingerprintString += cf.country || "";   // 国コード
  fingerprintString += cf.city || "";      // 市町村
  fingerprintString += cf.region || "";    // 地域コード
  fingerprintString += cf.tlsCipher || ""; // TLS暗号スイート
  fingerprintString += cf.tlsVersion || ""; // TLSバージョン
  fingerprintString += cf.colo || ""; // データセンターコード (例: SIN, TYO)
  fingerprintString += cf.timezone || ""; // タイムゾーン
  fingerprintString += cf.longitude || ""; // 経度
  fingerprintString += cf.latitude || "";  // 緯度
  fingerprintString += cf.clientTcpRtt || ""; // TCP RTT
  fingerprintString += cf.httpProtocol || ""; // HTTPプロトコル (HTTP/1.1, HTTP/2, HTTP/3)
  fingerprintString += cf.tlsClientHelloLength || ""; // TLS Client Helloの長さ
  fingerprintString += cf.tlsClientRandom || ""; // TLS Client Randomのハッシュ
  fingerprintString += cf.tlsClientExtensionsSha1 || ""; // TLS Client ExtensionsのSHA1ハッシュ


  // 7. IPアドレスのサブネットの一部 (IPが変わっても同じネットワークからの可能性)
  const ip = headers.get("CF-Connecting-IP");
  if (ip) {
    if (ip.includes('.')) { // IPv4
      fingerprintString += ip.split('.').slice(0, 3).join('.'); // 例: 192.168.1.x -> 192.168.1
    } else if (ip.includes(':')) { // IPv6
      fingerprintString += ip.split(':').slice(0, 4).join(':'); // 例: 2001:db8:abcd:1234:: -> 2001:db8:abcd:1234
    }
  }

  // --- ハッシュ化 ---
  const encoder = new TextEncoder();
  const data = encoder.encode(fingerprintString);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  const fingerprint = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');

  return fingerprint;
}

// Durable Object内でparseLocaleが必要なため、ここに定義
// ユーティリティ関数は共有ファイルにまとめるのがベストだが、ここではFingerprintTracker.js内に含める
function parseLocale(path) {
  const trimmedPath = path.replace(/^\/+/, "").toLowerCase();
  const seg = trimmedPath.split("/")[0];

  // --- 特例：日本向けURL（トップ, /ja, /en） ---
  if (seg === "" || seg === "ja") {
    return { lang: "ja", country: "jp" };
  }
  if (seg === "en") {
    return { lang: "en", country: "jp" };
  }

  // --- 通常ロケール: xx-XX 形式 ---
  const match = seg.match(/^([a-z]{2})-([a-z]{2})$/i);
  if (match) {
    return { lang: match[1], country: match[2] };
  }

  // --- 不明ロケール ---
  return { lang: "unknown", country: "unknown" };
}
