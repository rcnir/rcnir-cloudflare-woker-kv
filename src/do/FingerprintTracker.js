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
        // stateDataに全ての状態を集約し、単一のキーで管理
        this.state.blockConcurrencyWhile(async () => {
            this.stateData = (await this.state.storage.get("state")) || {
                count: 0, // このフィンガープリントID全体の違反カウント
                lastAccessTime: 0,
                requestCount: 0, // 短期間のリクエストカウント
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
        // Durable Objectsのライフサイクル管理は複雑なため、念のためのチェックは有効
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
                // 外部（メインWorker）から明示的に違反が通知された場合のロジック
                // 例: UAベースの既知のボット検知など
                let currentCount = this.stateData.count;
                currentCount++;
                this.stateData.count = currentCount;
                await this.state.storage.put("state", this.stateData);
                return new Response(JSON.stringify({ count: currentCount }), { headers: { 'Content-Type': 'application/json' } });
            }

            case "/check-locale-fp": {
                // フィンガープリントIDベースのロケールファンアウトチェック
                const { path } = await request.json(); // メインWorkerからpathを受信

                const { lang, country } = parseLocale(path);

                // 'unknown'なロケールは処理対象外とする (システムパスなど)
                if (lang === "unknown" || country === "unknown") {
                    return new Response(JSON.stringify({ violation: false, count: this.stateData.localeViolationCount }), {
                        headers: { "Content-Type": "application/json" }
                    });
                }

                let violationDetected = false;
                const LOCALE_WINDOW_MS = 10 * 1000; // ロケールファンアウトの判定ウィンドウ (10秒)

                // 前回のロケールと異なる、かつ短時間でのアクセスであれば違反
                if (this.stateData.lastLocale && this.stateData.lastLocale !== `${lang}-${country}`) {
                    if (now - this.stateData.lastLocaleTime < LOCALE_WINDOW_MS) {
                        violationDetected = true;
                        this.stateData.localeViolationCount++; // ロケール違反カウント増加
                        // 全体カウントは、各種類の違反カウントの最大値にするか、単純加算するか検討
                        // ここでは現状維持で、後でメインWorker側でMath.maxを使用
                    }
                }
                
                this.stateData.lastLocale = `${lang}-${country}`;
                this.stateData.lastLocaleTime = now;
                
                await this.state.storage.put("state", this.stateData); // 状態を保存

                return new Response(JSON.stringify({ violation: violationDetected, count: this.stateData.localeViolationCount }), { headers: { 'Content-Type': 'application/json' } });
            }

            case "/track-behavior": {
                // ★ フィンガープリントIDごとの行動パターン追跡と違反検知ロジック ★
                const { path } = await request.json();

                // 1. レート制限のチェックと更新 (高速アクセス検知)
                if (now - this.stateData.lastResetTime > RATE_LIMIT_WINDOW_MS) {
                    this.stateData.requestCount = 0;
                    this.stateData.lastResetTime = now;
                }
                this.stateData.requestCount++;
                if (this.stateData.requestCount > RATE_LIMIT_THRESHOLD) {
                    console.log(`[FP_BEHAVIOR_VIOLATION] High request rate for FP. Requests in window: ${this.stateData.requestCount}`);
                    // 違反カウントを増加 (FP全体のカウント)
                    // この増加分もメインWorker側で effectiveCount に考慮される
                    this.stateData.count++; 
                }

                // 2. パス履歴の追跡と不自然な遷移のチェック (多岐にわたるパスアクセス検知)
                this.stateData.pathHistory.push({ path, timestamp: now });
                // 古い履歴を削除 (指定ウィンドウ外のアクセスをクリア)
                this.stateData.pathHistory = this.stateData.pathHistory.filter(entry => now - entry.timestamp < PATH_HISTORY_WINDOW_MS);

                const uniquePaths = new Set(this.stateData.pathHistory.map(entry => entry.path));
                if (uniquePaths.size > PATH_HISTORY_THRESHOLD) {
                    console.log(`[FP_BEHAVIOR_VIOLATION] Too many unique paths for FP. Unique paths: ${uniquePaths.size}`);
                    // 違反カウントを増加
                    this.stateData.count++; 
                }

                this.stateData.lastAccessTime = now; // 最終アクセス時刻を更新
                await this.state.storage.put("state", this.stateData); // 更新された状態を永続化

                // メインWorkerに現在の状態を返す
                return new Response(JSON.stringify({
                    count: this.stateData.count, // FP全体の現在の違反カウント
                    requestCount: this.stateData.requestCount,
                    uniquePaths: uniquePaths.size
                }), { headers: { 'Content-Type': 'application/json' } });
            }

            case "/list-high-count-fp": {
                // Cron Triggerから呼び出され、高カウントのFPをリストアップするエンドポイント
                // FingerprintTrackerの全インスタンスのデータをリストアップするわけではない点に注意
                // 'sync-job-fp'という固定IDのDOインスタンスがリストアップを行う
                const data = await this.state.storage.list({ limit: 10000 }); // 全てのフィンガープリントIDの状態を取得
                const highCountFpIds = [];
                for (const [key, state] of data.entries()) {
                    // keyはフィンガープリントIDではない。各DOインスタンスは自身のIDをキーとして持たない。
                    // 実際には、"state"というキーで保存されたthis.stateDataからcountを取り出す
                    if (key === "state" && state && state.count >= 4) { // このDOインスタンスのカウントが4以上か
                        // このDOのID（フィンガープリントID）自体を取得する必要がある
                        // Durable Objectは自身のIDを知ることができる
                        const fpIdFromDO = this.state.id.toString(); // Durable ObjectのIDはString型
                        highCountFpIds.push(fpIdFromDO);
                    }
                }
                // このリストは本来、Cron Triggerを呼び出すWorkerから来るべき。
                // ここでは単一のDOインスタンス（sync-job-fp）のカウントを返す
                return new Response(JSON.stringify(highCountFpIds), {
                  headers: { "Content-Type": "application/json" }
                });
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

  // 1. User-Agent のコア部分 (揺れを吸収するために簡略化)
  const ua = headers.get("User-Agent") || "";
  // 例: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36
  const uaMatch = ua.match(/(Chrome)\/(\d+)\./i); // Chrome/120
  const osMatch = ua.match(/(Windows NT \d+\.\d+|Macintosh; Intel Mac OS X \d+_\d+_\d+|Linux)/i); // Windows NT 10.0, Mac OS X 10_15_7
  
  if (uaMatch && uaMatch[1] && uaMatch[2]) {
      fingerprintString += `UA:${uaMatch[1]}-${uaMatch[2]}`; // 例: UA:Chrome-120
  } else {
      fingerprintString += `UA:${ua}`; // マッチしない場合は全体を使う (フォールバック)
  }
  if (osMatch && osMatch[0]) {
      fingerprintString += `_OS:${osMatch[0].replace(/ /g, '_')}`; // 例: _OS:Macintosh;_Intel_Mac_OS_X_10_15_7
  }
  
  // 2. Accept ヘッダー群
  fingerprintString += `|AL:${headers.get("Accept-Language") || ""}`;
  fingerprintString += `|AE:${headers.get("Accept-Encoding") || ""}`;
  fingerprintString += `|A:${headers.get("Accept") || ""}`;

  // 3. Client Hints (存在すれば) - これらは非常に強力
  fingerprintString += `|SCU:${headers.get("Sec-Ch-Ua") || ""}`;
  fingerprintString += `|SCUM:${headers.get("Sec-Ch-Ua-Mobile") || ""}`;
  fingerprintString += `||SCUP:${headers.get("Sec-Ch-Ua-Platform") || ""}`; // typo修正

  // 4. Sec-Fetch ヘッダー群 - これらも強力
  fingerprintString += `|SFS:${headers.get("Sec-Fetch-Site") || ""}`;
  fingerprintString += `|SFM:${headers.get("Sec-Fetch-Mode") || ""}`;
  fingerprintString += `|SFD:${headers.get("Sec-Fetch-Dest") || ""}`;
  fingerprintString += `|SFU:${headers.get("Sec-Fetch-User") || ""}`;

  // 5. Referer (ある場合)
  fingerprintString += `|R:${headers.get("Referer") || ""}`;
  fingerprintString += `|UIR:${headers.get("Upgrade-Insecure-Requests") || ""}`;


  // 6. Cloudflare メタデータ (request.cf) - ネットワーク層の特性
  fingerprintString += `|ASN:${cf.asn || ""}`;       // AS番号
  fingerprintString += `|C:${cf.country || ""}`;   // 国コード
  fingerprintString += `|TZ:${cf.timezone || ""}`;  // タイムゾーン

  // TLS関連情報 - ボットが偽装しにくい低レベル情報
  fingerprintString += `|TC:${cf.tlsCipher || ""}`; // TLS暗号スイート
  fingerprintString += `|TV:${cf.tlsVersion || ""}`; // TLSバージョン
  fingerprintString += `|TCHL:${cf.tlsClientHelloLength || ""}`; // TLS Client Helloの長さ
  // ログにあるより詳細なTLSフィンガープリントも追加 (必要であれば)
  fingerprintString += `|TCSR:${cf.tlsClientRandom || ""}`; // TLS Client Random
  fingerprintString += `|TCE1:${cf.tlsClientExtensionsSha1 || ""}`; // TLS Client Extensions SHA1
  fingerprintString += `|TCE1LE:${cf.tlsClientExtensionsSha1Le || ""}`; // TLS Client Extensions SHA1 LE

  // IPアドレスのサブネットの一部 (フィンガープリントの安定性を損ねるが、サブネットレベルでの識別強化)
  const ip = headers.get("CF-Connecting-IP");
  if (ip) {
    if (ip.includes('.')) { // IPv4
      fingerprintString += `|IPS:${ip.split('.').slice(0, 3).join('.')}`; // 例: 192.168.1.x -> 192.168.1
    } else if (ip.includes(':')) { // IPv6
      fingerprintString += `|IPS6:${ip.split(':').slice(0, 4).join(':')}`; // 例: 2001:db8:abcd:1234:: -> 2001:db8:abcd:1234
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
