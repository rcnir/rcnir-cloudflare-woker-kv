// 設定可能な定数
const RATE_LIMIT_WINDOW_MS = 10 * 1000; // 10秒
const RATE_LIMIT_THRESHOLD = 5;         // 10秒間に5リクエスト以上で違反

const PATH_HISTORY_WINDOW_MS = 60 * 1000; // 60秒
const PATH_HISTORY_THRESHOLD = 10;        // 60秒間に10個以上の異なるパスで違反

const LOCALE_WINDOW_MS = 10 * 1000; // ロケールファンアウトの判定ウィンドウ (10秒)
const SINGLE_LOCALE_ACCESS_WINDOW_MS = 5 * 1000; // 5秒
const SINGLE_LOCALE_ACCESS_THRESHOLD = 3;        // 5秒間に3回以上同じロケールにアクセスで違反 (既に履歴があるFP向け)


export class FingerprintTracker {
    constructor(state, env) {
        this.state = state;
        this.env = env;
        this.stateData = null; // 初期化はfetch内で非同期で行う

        // コンストラクタで初期状態をロード (fetchでの初回アクセスでロードする方式も可)
        this.state.blockConcurrencyWhile(async () => {
            this.stateData = (await this.state.storage.get("state")) || this._getInitialState();
        });
    }

    _getInitialState() {
        const now = Date.now();
        return {
            count: 0, // 全体の累積違反カウント (Main WorkerのeffectiveCountに影響)
            localeViolationCount: 0, // ロケールに関する違反カウント
            lgRegions: {}, // ロケールファンアウト検出用
            singleLocaleAccess: { // 1ロケール連続アクセス検知用
                count: 0,
                firstAccess: 0,
                locale: ''
            },
            
            // 行動パターン検知用
            lastAccessTime: 0, // 最終アクセス時刻
            requestCount: 0, // 短期間のリクエストカウント
            pathHistory: [], // { path: string, timestamp: number }[]
            lastResetTime: now, // レート制限のリセット時間

            // JS実行フラグ
            jsExecuted: false,
        };
    }

    async fetch(request) {
        const url = new URL(request.url);
        const localNow = Date.now();

        // 状態がまだロードされていなければロード
        if (!this.stateData) {
            this.stateData = await this.state.storage.get("state") || this._getInitialState();
        }

        // 内部APIエンドポイントの処理
        switch (url.pathname) {
            case "/track-violation": {
                // 外部（メインWorker）から明示的に違反が通知された場合のロジック (例: UAベースの既知のボット検知)
                this.stateData.count++; // 違反カウント増加
                await this.state.storage.put("state", this.stateData);
                return new Response(JSON.stringify({ count: this.stateData.count }), { headers: { 'Content-Type': 'application/json' } });
            }

            case "/check-locale-fp": {
                // フィンガープリントIDベースのロケールファンアウトチェック
                const { path } = await request.json(); // メインWorkerからpathを受信

                const { lang, country } = parseLocale(path);

                // 'unknown'なロケールは処理対象外とする (システムパスなど)
                if (lang === "unknown" || country === "unknown") {
                    await this.state.storage.put("state", this.stateData); // 状態を保存（アクセス時刻の更新など）
                    return new Response(JSON.stringify({ violation: false, count: this.stateData.localeViolationCount }), {
                        headers: { "Content-Type": "application/json" }
                    });
                }

                let violationDetected = false;
                
                // 既存の複数ロケール検知ロジック (国セットのサイズで判断)
                this.stateData.lgRegions = this.stateData.lgRegions || {}; // stateDataにlgRegionsがなければ初期化
                for (const [key, ts] of Object.entries(this.stateData.lgRegions)) {
                    if (localNow - ts > LOCALE_WINDOW_MS) { // localNowを使用
                        delete this.stateData.lgRegions[key];
                    }
                }

                const currentLocaleKey = `${lang}-${country}`;
                this.stateData.lgRegions[currentLocaleKey] = localNow; // 最新のロケールアクセスを記録 (localNowを使用)

                const countries = new Set(Object.keys(this.stateData.lgRegions).map(k => k.split("-")[1]));
                if (countries.size >= 2) { // 複数国にまたがるアクセス
                    violationDetected = true;
                    this.stateData.localeViolationCount++; // ロケール違反カウント増加
                    this.stateData.count++; // 全体カウントも増加
                    this.stateData.lgRegions = {}; // 違反時は履歴をリセット
                }
                
                // 既存違反履歴がある場合の単一ロケール連続アクセス検知
                if (this.stateData.count >= 1 && !violationDetected) {
                    if (this.stateData.singleLocaleAccess.locale !== currentLocaleKey || localNow - this.stateData.singleLocaleAccess.firstAccess > SINGLE_LOCALE_ACCESS_WINDOW_MS) { // localNowを使用
                        // ロケールが変わったか、ウィンドウを過ぎたらリセット
                        this.stateData.singleLocaleAccess = { count: 1, firstAccess: localNow, locale: currentLocaleKey }; // localNowを使用
                    } else {
                        // 同じロケールで連続アクセス
                        this.stateData.singleLocaleAccess.count++;
                    }

                    if (this.stateData.singleLocaleAccess.count > SINGLE_LOCALE_ACCESS_THRESHOLD) {
                        console.log(`[FP_BEHAVIOR_VIOLATION] High single locale access rate for FP: ${this.stateData.singleLocaleAccess.count} in ${SINGLE_LOCALE_ACCESS_WINDOW_MS/1000}s`);
                        violationDetected = true;
                        this.stateData.localeViolationCount++; // ロケール違反カウント増加
                        this.stateData.count++; // 全体カウントも増加
                        this.stateData.singleLocaleAccess = { count: 0, firstAccess: 0, locale: '' }; // リセット
                    }
                }

                await this.state.storage.put("state", this.stateData); // 状態を保存

                return new Response(JSON.stringify({ violation: violationDetected, count: this.stateData.localeViolationCount }), { headers: { 'Content-Type': 'application/json' } });
            }

            case "/track-behavior": {
                // フィンガープリントIDごとの行動パターン追跡と違反検知ロジック
                const { path } = await request.json();

                // 1. レート制限のチェックと更新 (高速アクセス検知)
                if (localNow - this.stateData.lastResetTime > RATE_LIMIT_WINDOW_MS) { // localNowを使用
                    this.stateData.requestCount = 0;
                    this.stateData.lastResetTime = localNow; // localNowを使用
                }
                this.stateData.requestCount++;
                if (this.stateData.requestCount > RATE_LIMIT_THRESHOLD) {
                    console.log(`[FP_BEHAVIOR_VIOLATION] High request rate for FP. Requests in window: ${this.stateData.requestCount}`);
                    this.stateData.count++; // 違反カウント増加
                }

                // 2. パス履歴の追跡と不自然な遷移のチェック (多岐にわたるパスアクセス検知)
                this.stateData.pathHistory.push({ path, timestamp: localNow }); // localNowを使用
                this.stateData.pathHistory = this.stateData.pathHistory.filter(entry => localNow - entry.timestamp < PATH_HISTORY_WINDOW_MS); // localNowを使用

                const uniquePaths = new Set(this.stateData.pathHistory.map(entry => entry.path));
                if (uniquePaths.size > PATH_HISTORY_THRESHOLD) {
                    console.log(`[FP_BEHAVIOR_VIOLATION] Too many unique paths for FP. Unique paths: ${uniquePaths.size}`);
                    this.stateData.count++; // 違反カウント増加
                }

                this.stateData.lastAccessTime = localNow; // localNowを使用
                await this.state.storage.put("state", this.stateData); // 状態を永続化

                return new Response(JSON.stringify({
                    count: this.stateData.count, // FP全体の違反カウント
                    requestCount: this.stateData.requestCount,
                    uniquePaths: uniquePaths.size
                }), { headers: { 'Content-Type': 'application/json' } });
            }

            case "/list-high-count-fp": {
                // このエンドポイントはCronではなくKVから読み取るように変更されるため、このDO内部では不要だが、
                // 互換性のため残すか、削除する
                // Cron Triggerから呼び出され、高カウントのFPをリストアップするエンドポイント
                // このロジックはメインWorkerのCronジョブで直接KVをリストする形に変更されたため、
                // このDOインスタンス（sync-job-fp）が他のDOの状態を知ることはない。
                // したがって、このエンドポイントは実際には利用されなくなる。
                // ここでは、自身の状態が永久ブロック閾値を超えていたら自身のIDを返す（ただし、メインWorkerはこれを使わない）
                const highCountFpIds = [];
                if (this.stateData && this.stateData.count >= 4) {
                    highCountFpIds.push(this.state.id.toString());
                }
                return new Response(JSON.stringify(highCountFpIds), {
                    headers: { "Content-Type": "application/json" }
                });
            }

            case "/get-state": {
                // デバッグ用: 現在の状態を取得
                // JS実行フラグも返す
                return new Response(JSON.stringify(this.stateData), { headers: { 'Content-Type': 'application/json' } });
            }

            case "/record-js-execution": { // JS実行を記録するエンドポイント (Monorail用)
                this.stateData.jsExecuted = true;
                await this.state.storage.put("state", this.stateData);
                console.log(`[FP_JS_EXEC] FP=${this.state.id.toString()} has executed JS (Monorail).`);
                return new Response("JS execution recorded.", { status: 200 });
            }

            case "/internal/record-js-execution-from-html": { // 新しいエンドポイント (HTML埋め込みJS用)
                this.stateData.jsExecuted = true;
                await this.state.storage.put("state", this.stateData);
                console.log(`[FP_JS_EXEC_HTML] FP=${this.state.id.toString()} has executed JS from HTML.`);
                return new Response("JS execution from HTML recorded.", { status: 200 });
            }

            case "/reset-state": {
                // フィンガープリントIDのデータをリセットするエンドポイント
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
  const uaMatch = ua.match(/(Chrome|Firefox)\/(\d+)\./i); // Chrome/120
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
  fingerprintString += `|SCUP:${headers.get("Sec-Ch-Ua-Platform") || ""}`;

  // 4. Sec-Fetch ヘッダー群 - これらも強力
  fingerprintString += `|SFS:${headers.get("Sec-Fetch-Site") || ""}`;
  fingerprintString += `|SFM:${headers.get("Sec-Fetch-Mode") || ""}`;
  fingerprintString += `|SFD:${headers.get("Sec-Fetch-Dest") || ""}`;
  fingerprintString += `|SFU:${headers.get("Sec-Fetch-User") || ""}`;

  // 5. Referer (ある場合)
  fingerprintString += `|R:${headers.get("Referer") || ""}`;
  fingerprintString += `|UIR:${headers.get("Upgrade-Insecure-Requests") || ""}`;


  // 6. Cloudflare メタデータ (request.cf) - ネットワーク層の特性
  // ここで安定しているものだけを残す
  fingerprintString += `|ASN:${cf.asn || ""}`;    // AS番号 (安定している)
  fingerprintString += `|C:${cf.country || ""}`;  // 国コード (安定している)
  fingerprintString += `|TZ:${cf.timezone || ""}`; // タイムゾーン (安定している)
  fingerprintString += `|COLO:${cf.colo || ""}`; // データセンターコード (安定している)
  fingerprintString += `|HP:${cf.httpProtocol || ""}`; // HTTPプロトコル (安定している)

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
