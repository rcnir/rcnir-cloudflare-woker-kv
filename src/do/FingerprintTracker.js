// 設定可能な定数
const RATE_LIMIT_WINDOW_MS = 10 * 1000; // 10秒
const RATE_LIMIT_THRESHOLD = 5;          // 10秒間に5リクエスト以上で違反

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

        this.state.blockConcurrencyWhile(async () => {
            this.stateData = (await this.state.storage.get("state")) || this._getInitialState();
        });
    }

    _getInitialState() {
        const now = Date.now();
        return {
            count: 0, 
            localeViolationCount: 0, 
            lgRegions: {}, 
            singleLocaleAccess: { 
                count: 0,
                firstAccess: 0,
                locale: ''
            },
            lastAccessTime: 0,
            requestCount: 0,
            pathHistory: [],
            lastResetTime: now,
            jsExecuted: false,
        };
    }

    async fetch(request) {
        const url = new URL(request.url);
        const localNow = Date.now();

        if (!this.stateData) {
            this.stateData = await this.state.storage.get("state") || this._getInitialState();
        }

        switch (url.pathname) {
            case "/track-violation": {
                this.stateData.count++;
                await this.state.storage.put("state", this.stateData);
                return new Response(JSON.stringify({ count: this.stateData.count }), { headers: { 'Content-Type': 'application/json' } });
            }

            case "/check-locale-fp": {
                const { path } = await request.json();
                const { lang, country } = parseLocale(path);

                if (lang === "unknown" || country === "unknown") {
                    await this.state.storage.put("state", this.stateData);
                    return new Response(JSON.stringify({ violation: false, count: this.stateData.localeViolationCount }), {
                        headers: { "Content-Type": "application/json" }
                    });
                }

                let violationDetected = false;
                
                this.stateData.lgRegions = this.stateData.lgRegions || {};
                for (const [key, ts] of Object.entries(this.stateData.lgRegions)) {
                    if (localNow - ts > LOCALE_WINDOW_MS) {
                        delete this.stateData.lgRegions[key];
                    }
                }

                const currentLocaleKey = `${lang}-${country}`;
                this.stateData.lgRegions[currentLocaleKey] = localNow;

                const countries = new Set(Object.keys(this.stateData.lgRegions).map(k => k.split("-")[1]));
                if (countries.size >= 2) {
                    violationDetected = true;
                    this.stateData.localeViolationCount++;
                    this.stateData.count++;
                    this.stateData.lgRegions = {};
                }
                
                if (this.stateData.count >= 1 && !violationDetected) {
                    if (this.stateData.singleLocaleAccess.locale !== currentLocaleKey || localNow - this.stateData.singleLocaleAccess.firstAccess > SINGLE_LOCALE_ACCESS_WINDOW_MS) {
                        this.stateData.singleLocaleAccess = { count: 1, firstAccess: localNow, locale: currentLocaleKey };
                    } else {
                        this.stateData.singleLocaleAccess.count++;
                    }

                    if (this.stateData.singleLocaleAccess.count > SINGLE_LOCALE_ACCESS_THRESHOLD) {
                        console.log(`[FP_BEHAVIOR_VIOLATION] High single locale access rate for FP: ${this.stateData.singleLocaleAccess.count} in ${SINGLE_LOCALE_ACCESS_WINDOW_MS/1000}s`);
                        violationDetected = true;
                        this.stateData.localeViolationCount++;
                        this.stateData.count++;
                        this.stateData.singleLocaleAccess = { count: 0, firstAccess: 0, locale: '' };
                    }
                }

                await this.state.storage.put("state", this.stateData);

                return new Response(JSON.stringify({ violation: violationDetected, count: this.stateData.localeViolationCount }), { headers: { 'Content-Type': 'application/json' } });
            }

            case "/track-behavior": {
                const { path } = await request.json();

                if (localNow - this.stateData.lastResetTime > RATE_LIMIT_WINDOW_MS) {
                    this.stateData.requestCount = 0;
                    this.stateData.lastResetTime = localNow;
                }
                this.stateData.requestCount++;
                if (this.stateData.requestCount > RATE_LIMIT_THRESHOLD) {
                    console.log(`[FP_BEHAVIOR_VIOLATION] High request rate for FP. Requests in window: ${this.stateData.requestCount}`);
                    this.stateData.count++;
                }

                this.stateData.pathHistory.push({ path, timestamp: localNow });
                this.stateData.pathHistory = this.stateData.pathHistory.filter(entry => localNow - entry.timestamp < PATH_HISTORY_WINDOW_MS);

                const uniquePaths = new Set(this.stateData.pathHistory.map(entry => entry.path));
                if (uniquePaths.size > PATH_HISTORY_THRESHOLD) {
                    console.log(`[FP_BEHAVIOR_VIOLATION] Too many unique paths for FP. Unique paths: ${uniquePaths.size}`);
                    this.stateData.count++;
                }

                this.stateData.lastAccessTime = localNow;
                await this.state.storage.put("state", this.stateData);

                return new Response(JSON.stringify({
                    count: this.stateData.count,
                    requestCount: this.stateData.requestCount,
                    uniquePaths: uniquePaths.size
                }), { headers: { 'Content-Type': 'application/json' } });
            }

            case "/list-high-count-fp": {
                const highCountFpIds = [];
                if (this.stateData && this.stateData.count >= 4) {
                    highCountFpIds.push(this.state.id.toString());
                }
                return new Response(JSON.stringify(highCountFpIds), {
                    headers: { "Content-Type": "application/json" }
                });
            }

            case "/get-state": {
                return new Response(JSON.stringify(this.stateData), { headers: { 'Content-Type': 'application/json' } });
            }

            case "/record-js-execution": {
                this.stateData.jsExecuted = true;
                await this.state.storage.put("state", this.stateData);
                console.log(`[FP_JS_EXEC] FP=${this.state.id.toString()} has executed JS (Monorail).`);
                return new Response("JS execution recorded.", { status: 200 });
            }

            case "/internal/record-js-execution-from-html": {
                this.stateData.jsExecuted = true;
                await this.state.storage.put("state", this.stateData);
                console.log(`[FP_JS_EXEC_HTML] FP=${this.state.id.toString()} has executed JS from HTML.`);
                return new Response("JS execution from HTML recorded.", { status: 200 });
            }

            case "/reset-state": {
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
    const cf = request.cf || {};

    // ★★★ フィンガープリントの構成要素を厳格に標準化 ★★★
    let fpParts = [];

    // 1. User-Agent (User-Agent文字列全体をそのまま使用し、欠損時は固定文字列)
    // User-Agentが変動しないというご指摘を重視し、そのまま使用します。
    // String()で明示的に文字列に変換し、trim()で余分な空白を除去。
    fpParts.push(`UA:${String(headers.get("User-Agent") || "UnknownUA").trim()}`);
    
    // 2. Cloudflare メタデータ (request.cf) - 最も安定した特性のみに絞る
    // ASN (ISP) と Country (国コード) のみを含めます。
    // String()で明示的に文字列に変換し、trim()で余分な空白を除去。
    fpParts.push(`ASN:${String(cf.asn || "UnknownASN").trim()}`);    
    fpParts.push(`C:${String(cf.country || "UnknownCountry").trim()}`);  

    // 他の全てのヘッダーやCFメタデータは、変動要因となるため完全に除外済み。

    // ★★★ 構成要素を厳密な文字列として結合 ★★★
    // 各要素間を固定の区切り文字で結合することで、順序や欠損による変動を防ぐ。
    const fingerprintString = fpParts.join('|');

    // --- ハッシュ化 ---
    // crypto.subtle.digest は、入力データが同じであれば必ず同じハッシュを生成します。
    // 問題はここではなく、入力の `fingerprintString` がリクエストごとに変動している点です。
    const encoder = new TextEncoder();
    const data = encoder.encode(fingerprintString);
    const hashBuffer = await crypto.subtle.digest('SHA-256', data);
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    const fingerprint = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');

    // ★★★ デバッグ用ログ出力を強化 ★★★
    // このログが、FP変動の最終的な原因を特定する鍵となります。
    console.log(`[FP_DEBUG] Original UA: "${headers.get("User-Agent") || "N/A"}"`);
    console.log(`[FP_DEBUG] Original ASN: "${cf.asn || "N/A"}", Original Country: "${cf.country || "N/A"}"`);
    console.log(`[FP_DEBUG] Constructed String: "${fingerprintString}" -> Generated FP: "${fingerprint}"`);

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
