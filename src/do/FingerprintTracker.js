// 設定可能な定数
const RATE_LIMIT_WINDOW_MS = 10 * 1000;
const RATE_LIMIT_THRESHOLD = 5;

const PATH_HISTORY_WINDOW_MS = 60 * 1000;
const PATH_HISTORY_THRESHOLD = 10;

const LOCALE_WINDOW_MS = 10 * 1000;
const SINGLE_LOCALE_ACCESS_WINDOW_MS = 5 * 1000;
const SINGLE_LOCALE_ACCESS_THRESHOLD = 3;

// CRC32の実装
function crc32(str) {
    let crc = -1;
    for (let i = 0; i < str.length; i++) {
        let char = str.charCodeAt(i);
        crc = (crc >>> 0) ^ char;
        for (let j = 0; j < 8; j++) {
            if ((crc & 1) === 1) {
                crc = (crc >>> 1) ^ 0xEDB88320;
            } else {
                crc = (crc >>> 1);
            }
        }
    }
    return (crc ^ -1) >>> 0;
}

export class FingerprintTracker {
    constructor(state, env) {
        this.state = state;
        this.env = env;
        this.stateData = null;

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
// ★ 変更: logBufferを引数として受け取る
export async function generateFingerprint(request, logBuffer) {
    const headers = request.headers;
    const cf = request.cf || {};

    let fpParts = [];

    // --- フィンガープリントのコア要素 ---
    fpParts.push(`UA:${String(headers.get("User-Agent") || "UnknownUA").trim()}`);
    fpParts.push(`ASN:${String(cf.asn || "UnknownASN").trim()}`);   
    fpParts.push(`C:${String(cf.country || "UnknownCountry").trim()}`);  
    fpParts.push(`AL:${String(headers.get("Accept-Language") || "N/A").trim()}`);
    fpParts.push(`SCP:${String(headers.get("Sec-Ch-Ua-Platform") || "N/A").trim()}`);
    fpParts.push(`TC:${String(cf.tlsCipher || "N/A").trim()}`);
    fpParts.push(`TV:${String(cf.tlsVersion || "N/A").trim()}`);
    fpParts.push(`TCS:${String(cf.tlsClientCiphersSha1 || "N/A").trim()}`);

    const fingerprintString = fpParts.join('|');
    const fingerprint = crc32(fingerprintString).toString(16).padStart(8, '0');

    // ★ 変更: すべてのconsole.logをlogBuffer.pushに置き換え
    logBuffer.push(`--- FP_FULL_DEBUG START ---`);
    logBuffer.push(`[FP_FULL_DEBUG] URL: ${request.url}`);
    logBuffer.push(`[FP_FULL_DEBUG] IP: ${headers.get("CF-Connecting-IP") || "N/A"}`);
    
    logBuffer.push(`[FP_FULL_DEBUG] Original UA: "${headers.get("User-Agent") || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] Original ASN: "${cf.asn || "N/A"}", Original Country: "${cf.country || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] Constructed String: "${fingerprintString}" -> Generated FP (CRC32): "${fingerprint}"`);

    logBuffer.push(`[FP_FULL_DEBUG] Headers - Accept: "${headers.get("Accept") || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] Headers - Accept-Encoding: "${headers.get("Accept-Encoding") || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] Headers - Accept-Language: "${headers.get("Accept-Language") || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] Headers - Sec-Ch-Ua: "${headers.get("Sec-Ch-Ua") || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] Headers - Sec-Ch-Ua-Mobile: "${headers.get("Sec-Ch-Ua-Mobile") || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] Headers - Sec-Ch-Ua-Platform: "${headers.get("Sec-Ch-Ua-Platform") || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] Headers - Sec-Fetch-Site: "${headers.get("Sec-Fetch-Site") || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] Headers - Sec-Fetch-Mode: "${headers.get("Sec-Fetch-Mode") || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] Headers - Sec-Fetch-Dest: "${headers.get("Sec-Fetch-Dest") || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] Headers - Sec-Fetch-User: "${headers.get("Sec-Fetch-User") || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] Headers - Referer: "${headers.get("Referer") || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] Headers - Upgrade-Insecure-Requests: "${headers.get("Upgrade-Insecure-Requests") || "N/A"}"`);

    logBuffer.push(`[FP_FULL_DEBUG] CF - Colo: "${cf.colo || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] CF - Timezone: "${cf.timezone || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] CF - HTTP Protocol: "${cf.httpProtocol || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] CF - TLS Cipher: "${cf.tlsCipher || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] CF - TLS Version: "${cf.tlsVersion || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] CF - Client Hello Length: "${cf.tlsClientHelloLength || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] CF - Client Random: "${cf.tlsClientRandom || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] CF - Client Ciphers Sha1: "${cf.tlsClientCiphersSha1 || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] CF - Client Extensions Sha1: "${cf.tlsClientExtensionsSha1 || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] CF - Client Extensions Sha1 Le: "${cf.tlsClientExtensionsSha1Le || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] CF - Client TCP RTT: "${cf.clientTcpRtt || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] CF - Longitude: "${cf.longitude || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] CF - Latitude: "${cf.latitude || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] CF - City: "${cf.city || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] CF - Region: "${cf.region || "N/A"}"`);
    logBuffer.push(`[FP_FULL_DEBUG] CF - Postal Code: "${cf.postalCode || "N/A"}"`);

    logBuffer.push(`--- FP_FULL_DEBUG END ---`);

    return fingerprint;
}

// Durable Object内でparseLocaleが必要なため、ここに定義
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
