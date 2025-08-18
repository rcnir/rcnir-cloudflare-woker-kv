// スコア減衰の設定 (点/分)
const SCORE_DECAY_PER_MINUTE = 1;
const DECAY_INTERVAL_MS = 60 * 1000;

// ★ 修正: crc32関数を再度定義
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

// ★ 修正: index.jsがインポートするために、generateFingerprint関数を再度エクスポート
export async function generateFingerprint(request, logBuffer) {
    const headers = request.headers;
    const cf = request.cf || {};
    let fpParts = [];

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

    logBuffer.push(`--- FP_FULL_DEBUG START ---`);
    logBuffer.push(`[FP_FULL_DEBUG] URL: ${request.url}`);
    logBuffer.push(`[FP_FULL_DEBUG] IP: ${headers.get("CF-Connecting-IP") || "N/A"}`);
    logBuffer.push(`[FP_FULL_DEBUG] Constructed String: "${fingerprintString}" -> Generated FP (CRC32): "${fingerprint}"`);
    logBuffer.push(`--- FP_FULL_DEBUG END ---`);

    return fingerprint;
}

export class FingerprintTrackerV2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.memState = null;

    this.state.blockConcurrencyWhile(async () => {
      this.memState = await this.state.storage.get("state") || this._getInitialState();
    });
  }

  _getInitialState() {
    return {
      score: 0,
      lastUpdated: Date.now(),
      hasStrike: false,
      jsExecuted: false,
      lgRegions: {}
    };
  }

  async fetch(request) {
    if (!this.memState) {
        this.memState = await this.state.storage.get("state") || this._getInitialState();
    }
    const url = new URL(request.url);
    switch (url.pathname) {
      case "/update-score":
        return this.handleUpdateScore(request);
      case "/record-js-execution":
        return this.recordJsExecution();
      case "/check-locale-fp":
        return this.handleLocaleCheck(request);
      case "/get-state":
        return new Response(JSON.stringify(this.memState), { headers: { 'Content-Type': 'application/json' } });
      default:
        return new Response("Not found", { status: 404 });
    }
  }

  async recordJsExecution() {
    if (!this.memState.jsExecuted) {
      this.memState.jsExecuted = true;
      await this.state.storage.put("state", this.memState);
    }
    console.log(`[DO_JS_RECORD] FP=${this.state.id.toString()} recorded JS execution.`);
    return new Response("JS execution recorded.", { status: 200 });
  }
  
  async handleUpdateScore(request) {
    const { scoreToAdd, config } = await request.json();
    const now = Date.now();
    const minutesPassed = Math.floor((now - this.memState.lastUpdated) / DECAY_INTERVAL_MS);
    if (minutesPassed > 0) {
      this.memState.score = Math.max(0, this.memState.score - (minutesPassed * SCORE_DECAY_PER_MINUTE));
    }
    this.memState.score += scoreToAdd;
    this.memState.lastUpdated = now;

    let action = "ALLOW";
    const blockThreshold = config?.thresholds?.block ?? 70;
    const challengeThreshold = config?.thresholds?.challenge ?? 40;

    if (this.memState.score >= blockThreshold) {
      action = "BLOCK";
    } else if (this.memState.score >= challengeThreshold) {
      action = "CHALLENGE";
    }

    if (action === "BLOCK") {
      if (this.memState.hasStrike) {
        action = "PERMANENT_BLOCK";
      } else {
        this.memState.hasStrike = true;
        action = "TEMP_BLOCK";
      }
    }
    await this.state.storage.put("state", this.memState);
    return new Response(JSON.stringify({ newScore: this.memState.score, action }), {
      headers: { 'Content-Type': 'application/json' }
    });
  }

  async handleLocaleCheck(request) {
    const { path, config, country } = await request.json();
    const { lang, country: pathCountry } = parseLocale(path);
    if (lang === "unknown" || pathCountry === "unknown") {
      return new Response(JSON.stringify({ violation: false }));
    }
    const now = Date.now();
    const window = 10 * 1000;
    this.memState.lgRegions = this.memState.lgRegions || {};
    for (const [key, ts] of Object.entries(this.memState.lgRegions)) {
        if (now - ts > window) delete this.memState.lgRegions[key];
    }
    const currentKey = `${lang}-${pathCountry}`;
    this.memState.lgRegions[currentKey] = now;
    const visitedLanguages = new Set(Object.keys(this.memState.lgRegions).map(k => k.split("-")[0]));
    
    const multiLangConfig = config?.multiLanguageCountries?.[country];
    if (multiLangConfig) {
      const isSubset = [...visitedLanguages].every(l => multiLangConfig.includes(l));
      if (isSubset) {
        await this.state.storage.put("state", this.memState);
        return new Response(JSON.stringify({ violation: false, multiLangRule: true }));
      }
    }

    const violation = visitedLanguages.size >= 3;
    if(violation) this.memState.lgRegions = {};
    await this.state.storage.put("state", this.memState);
    return new Response(JSON.stringify({ violation }), { headers: { 'Content-Type': 'application/json' } });
  }
}

function parseLocale(path) {
  const trimmedPath = path.replace(/^\/+/, "").toLowerCase();
  const seg = trimmedPath.split("/")[0];

  if (seg === "" || seg === "ja") return { lang: "ja", country: "jp" };
  if (seg === "en") return { lang: "en", country: "jp" };

  const match = seg.match(/^([a-z]{2})-([a-z]{2})$/i);
  if (match) return { lang: match[1], country: match[2] };

  return { lang: "unknown", country: "unknown" };
}
