// src/do/FingerprintTracker.js

/**
 * FingerprintTrackerV2 (Durable Object)
 * - 無料枠で破綻しないよう、Durable Objects の永続ストレージ(state.storage)を一切使わない版。
 * - locale fanout 判定はメモリのみで保持。
 * - "JS実行済み" は DO ではなくメインWorker側で KV に記録する（index.js側）。
 */

const SCORE_DECAY_PER_MINUTE = 1;
const DECAY_INTERVAL_MS = 60 * 1000;

// --- 指紋生成に使う軽量ハッシュ: CRC32（簡易実装） ---
function crc32(str) {
  let crc = -1;
  for (let i = 0; i < str.length; i++) {
    let char = str.charCodeAt(i);
    crc = (crc >>> 0) ^ char;
    for (let j = 0; j < 8; j++) {
      if ((crc & 1) === 1) crc = (crc >>> 1) ^ 0xEDB88320;
      else crc = (crc >>> 1);
    }
  }
  return (crc ^ -1) >>> 0;
}

// --- index.js からも利用するフィンガープリント生成関数 ---
export async function generateFingerprint(request, logBuffer) {
  const headers = request.headers;
  const cf = request.cf || {};
  const fpParts = [];

  fpParts.push(`UA:${String(headers.get("User-Agent") || "UnknownUA").trim()}`);
  fpParts.push(`ASN:${String(cf.asn || "UnknownASN").trim()}`);
  fpParts.push(`C:${String(cf.country || "UnknownCountry").trim()}`);
  fpParts.push(`AL:${String(headers.get("Accept-Language") || "N/A").trim()}`);
  fpParts.push(`SCP:${String(headers.get("Sec-Ch-Ua-Platform") || "N/A").trim()}`);
  fpParts.push(`TC:${String(cf.tlsCipher || "N/A").trim()}`);
  fpParts.push(`TV:${String(cf.tlsVersion || "N/A").trim()}`);
  fpParts.push(`TCS:${String(cf.tlsClientCiphersSha1 || "N/A").trim()}`);

  const fingerprintString = fpParts.join("|");
  const fingerprint = crc32(fingerprintString).toString(16).padStart(8, "0");

  if (logBuffer) {
    logBuffer.push(`--- FP_FULL_DEBUG START ---`);
    logBuffer.push(`[FP_FULL_DEBUG] URL: ${request.url}`);
    logBuffer.push(`[FP_FULL_DEBUG] IP: ${headers.get("CF-Connecting-IP") || "N/A"}`);
    logBuffer.push(`[FP_FULL_DEBUG] Constructed String: "${fingerprintString}" -> Generated FP (CRC32): "${fingerprint}"`);
    logBuffer.push(`--- FP_FULL_DEBUG END ---`);
  }

  return fingerprint;
}

export class FingerprintTrackerV2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    // 永続化しない（メモリのみ）
    this.memState = this._getInitialState();
  }

  _getInitialState() {
    return {
      score: 0,
      lastUpdated: Date.now(),
      hasStrike: false,

      // locale fanout tracking (in-memory only)
      lgRegions: {}, // key: "lang-country" -> timestamp(ms)
    };
  }

  async fetch(request) {
    const url = new URL(request.url);

    switch (url.pathname) {
      case "/update-score":
        return this.handleUpdateScore(request);

      case "/check-locale-fp":
        return this.handleLocaleCheck(request);

      case "/get-state":
        return new Response(JSON.stringify(this.memState), {
          headers: { "Content-Type": "application/json" },
        });

      default:
        return new Response("Not found", { status: 404 });
    }
  }

  async handleUpdateScore(request) {
    let body;
    try {
      body = await request.json();
    } catch {
      return new Response("bad json", { status: 400 });
    }

    const { scoreToAdd, config } = body || {};
    const now = Date.now();
    const minutesPassed = Math.floor((now - this.memState.lastUpdated) / DECAY_INTERVAL_MS);

    if (minutesPassed > 0) {
      this.memState.score = Math.max(0, this.memState.score - minutesPassed * SCORE_DECAY_PER_MINUTE);
    }
    this.memState.score += Number(scoreToAdd) || 0;
    this.memState.lastUpdated = now;

    let action = "ALLOW";
    const blockThreshold = config?.thresholds?.block ?? 70;
    const challengeThreshold = config?.thresholds?.challenge ?? 40;

    if (this.memState.score >= blockThreshold) action = "BLOCK";
    else if (this.memState.score >= challengeThreshold) action = "CHALLENGE";

    if (action === "BLOCK") {
      if (this.memState.hasStrike) action = "PERMANENT_BLOCK";
      else {
        this.memState.hasStrike = true;
        action = "TEMP_BLOCK";
      }
    }

    // ★永続化しない：state.storage.put しない
    return new Response(JSON.stringify({ newScore: this.memState.score, action }), {
      headers: { "Content-Type": "application/json" },
    });
  }

  async handleLocaleCheck(request) {
    let body;
    try {
      body = await request.json();
    } catch {
      return new Response("bad json", { status: 400 });
    }

    const { path, config, country } = body || {};
    const { lang, country: pathCountry } = parseLocale(String(path || "/"));

    if (lang === "unknown" || pathCountry === "unknown") {
      return new Response(JSON.stringify({ violation: false }), {
        headers: { "Content-Type": "application/json" },
      });
    }

    const now = Date.now();
    const windowMs = 10 * 1000;

    // 掃除（期限切れを削除）
    this.memState.lgRegions = this.memState.lgRegions || {};
    for (const [key, ts] of Object.entries(this.memState.lgRegions)) {
      if (now - ts > windowMs) delete this.memState.lgRegions[key];
    }

    // 今回の訪問（メモリのみ）
    const currentKey = `${lang}-${pathCountry}`;
    this.memState.lgRegions[currentKey] = now;

    const visitedLanguages = new Set(
      Object.keys(this.memState.lgRegions).map((k) => k.split("-")[0])
    );

    // 多言語国家の特別ルール
    const multiLangConfig = config?.multiLanguageCountries?.[country];
    if (multiLangConfig) {
      const isSubset = [...visitedLanguages].every((l) => multiLangConfig.includes(l));
      if (isSubset) {
        return new Response(JSON.stringify({ violation: false, multiLangRule: true }), {
          headers: { "Content-Type": "application/json" },
        });
      }
    }

    // 違反判定（元ロジック踏襲：3言語以上）
    const violation = visitedLanguages.size >= 3;
    if (violation) this.memState.lgRegions = {};

    return new Response(JSON.stringify({ violation }), {
      headers: { "Content-Type": "application/json" },
    });
  }
}

function parseLocale(path) {
  const trimmedPath = String(path || "/").replace(/^\/+/, "").toLowerCase();
  const seg = trimmedPath.split("/")[0];

  if (seg === "" || seg === "ja") return { lang: "ja", country: "jp" };
  if (seg === "en") return { lang: "en", country: "jp" };

  const match = seg.match(/^([a-z]{2})-([a-z]{2})$/i);
  if (match) return { lang: match[1], country: match[2] };

  return { lang: "unknown", country: "unknown" };
}
