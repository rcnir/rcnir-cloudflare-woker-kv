/*
 * =================================================================
 * 目次 (Table of Contents)
 * =================================================================
 * 1. エクスポートクラス宣言 (IPStateTracker)
 * 2. fetch: DOエントリポイント
 * 3. 各種ハンドラ (Score, RateLimit, Locale)
 * 4. parseLocale: ユーティリティ
 * =================================================================
 */

// スコア減衰の設定 (点/分)
const SCORE_DECAY_PER_MINUTE = 1;
const DECAY_INTERVAL_MS = 60 * 1000;

export class IPStateTracker {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.memState = null; // メモリ内キャッシュ

    this.state.blockConcurrencyWhile(async () => {
      this.memState = await this.state.storage.get("state") || this._getInitialState();
    });
  }

  _getInitialState() {
    return {
      score: 0,
      lastUpdated: Date.now(),
      hasStrike: false, // 再犯ルール用のフラグ
      rateLimit: { count: 0, firstRequest: 0 },
      lgRegions: {}
    };
  }

  async fetch(request) {
    // 確実にメモリ内キャッシュを初期化
    if (!this.memState) {
        this.memState = await this.state.storage.get("state") || this._getInitialState();
    }

    const url = new URL(request.url);
    switch (url.pathname) {
      case "/update-score":
        return this.handleUpdateScore(request);
      case "/rate-limit":
        return this.handleRateLimit(request);
      case "/check-locale":
        return this.handleLocaleCheck(request);
      case "/list-high-count": // cron用
        return this.listHighViolationIps();
      case "/get-state": // デバッグ用
        return new Response(JSON.stringify(this.memState), { headers: { 'Content-Type': 'application/json' } });
      default:
        return new Response("Not found", { status: 404 });
    }
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
  
  async handleRateLimit(request) {
    const now = Date.now();
    const duration = 60 * 1000;
    const limit = 10;
    
    if (now - (this.memState.rateLimit?.firstRequest || 0) > duration) {
      this.memState.rateLimit = { count: 1, firstRequest: now };
    } else {
      this.memState.rateLimit.count++;
    }
    await this.state.storage.put("state", this.memState);
    const allowed = this.memState.rateLimit.count <= limit;
    return new Response(JSON.stringify({ allowed }), { headers: { 'Content-Type': 'application/json' } });
  }

  async handleLocaleCheck(request) {
    const { path } = await request.json();
    const { lang, country } = parseLocale(path);
    if (lang === "unknown" || country === "unknown") {
      return new Response(JSON.stringify({ violation: false }));
    }
    const now = Date.now();
    const window = 10 * 1000; // 10秒
    const threshold = 3; // 3カ国以上
    
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

  async listHighViolationIps() {
    const data = await this.state.storage.list({ limit: 10000 });
    const highCountIps = [];
    for (const key of data.keys()) {
        const state = await data.get(key);
        if (state && state.hasStrike) {
            highCountIps.push(key);
        }
    }
    return new Response(JSON.stringify(highCountIps), { headers: { "Content-Type": "application/json" } });
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
