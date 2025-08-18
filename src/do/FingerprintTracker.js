// スコア減衰の設定 (点/分)
const SCORE_DECAY_PER_MINUTE = 1;
const DECAY_INTERVAL_MS = 60 * 1000;

// generateFingerprint と parseLocale は index.js 側で定義・使用されるため、
// このファイル内での重複定義は不要です。
// もし、このDOファイル単体でテストしたい場合は、以下のコメントアウトを解除してください。
/*
function crc32(str) { ... }
export async function generateFingerprint(request, logBuffer) { ... }
function parseLocale(path) { ... }
*/

export class FingerprintTracker {
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
      hasStrike: false, // 再犯ルール用のフラグ
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
    // この関数はIPStateTrackerの同名関数とほぼ同じロジックです
    const { path } = await request.json();
    const { lang, country } = parseLocale(path);
    if (lang === "unknown" || country === "unknown") {
      return new Response(JSON.stringify({ violation: false }));
    }
    const now = Date.now();
    const window = 10 * 1000;
    const threshold = 3;
    
    this.memState.lgRegions = this.memState.lgRegions || {};
    for (const [key, ts] of Object.entries(this.memState.lgRegions)) {
        if (now - ts > window) delete this.memState.lgRegions[key];
    }
    
    const currentKey = `${lang}-${country}`;
    this.memState.lgRegions[currentKey] = now;
    
    const countries = new Set(Object.keys(this.memState.lgRegions).map(k => k.split("-")[1]));
    const violation = countries.size >= threshold;

    if(violation) this.memState.lgRegions = {};
    
    await this.state.storage.put("state", this.memState);
    return new Response(JSON.stringify({ violation }), { headers: { 'Content-Type': 'application/json' } });
  }
}

// このファイル内で `parseLocale` が必要なため、ここにも定義します。
function parseLocale(path) {
  const trimmedPath = path.replace(/^\/+/, "").toLowerCase();
  const seg = trimmedPath.split("/")[0];

  if (seg === "" || seg === "ja") return { lang: "ja", country: "jp" };
  if (seg === "en") return { lang: "en", country: "jp" };

  const match = seg.match(/^([a-z]{2})-([a-z]{2})$/i);
  if (match) return { lang: match[1], country: match[2] };

  return { lang: "unknown", country: "unknown" };
}
