// src/do/IPStateTracker.js

/**
 * IPStateTrackerV2 (Durable Object)
 * - 無料枠で破綻しないよう、Durable Objects の永続ストレージ(state.storage)を一切使わない版。
 * - rateLimit / locale fanout は「短時間窓」なのでメモリのみで十分。
 * - DO プロセスが落ちれば状態は消えるが、無料運用の安定性を優先。
 */

export class IPStateTrackerV2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    // 永続化しない（メモリのみ）
    this.memState = this._getInitialState();
  }

  _getInitialState() {
    return {
      // rate limit (in-memory only)
      rateLimit: { count: 0, firstRequest: 0 },

      // locale fanout tracking (in-memory only)
      lgRegions: {}, // key: "lang-country" -> timestamp(ms)
    };
  }

  async fetch(request) {
    const url = new URL(request.url);
    switch (url.pathname) {
      case "/rate-limit":
        return this.handleRateLimit(request);

      case "/check-locale":
        return this.handleLocaleCheck(request);

      case "/get-state":
        return new Response(JSON.stringify(this.memState), {
          headers: { "Content-Type": "application/json" },
        });

      default:
        return new Response("Not found", { status: 404 });
    }
  }

  async handleRateLimit(_request) {
    // 1分窓、10回まで（safe bot 用）
    const now = Date.now();
    const duration = 60 * 1000;
    const limit = 10;

    if (now - (this.memState.rateLimit?.firstRequest || 0) > duration) {
      this.memState.rateLimit = { count: 1, firstRequest: now };
    } else {
      this.memState.rateLimit.count++;
    }

    const allowed = this.memState.rateLimit.count <= limit;
    return new Response(JSON.stringify({ allowed }), {
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
    const windowMs = 10 * 1000; // 10秒窓

    // 掃除（期限切れを削除）
    this.memState.lgRegions = this.memState.lgRegions || {};
    for (const [key, ts] of Object.entries(this.memState.lgRegions)) {
      if (now - ts > windowMs) delete this.memState.lgRegions[key];
    }

    // 今回の訪問を記録（メモリのみ）
    const currentKey = `${lang}-${pathCountry}`;
    this.memState.lgRegions[currentKey] = now;

    const visitedLanguages = new Set(
      Object.keys(this.memState.lgRegions).map((k) => k.split("-")[0])
    );

    // 多言語国家の特別ルール
    const multiLangConfig = config?.multiLanguageCountries?.[country];
    if (multiLangConfig) {
      const isSubset = [...visitedLanguages].every((l) =>
        multiLangConfig.includes(l)
      );
      if (isSubset) {
        return new Response(JSON.stringify({ violation: false, multiLangRule: true }), {
          headers: { "Content-Type": "application/json" },
        });
      }
    }

    // 違反判定（元ロジック踏襲：3言語以上）
    const violation = visitedLanguages.size >= 3;
    if (violation) this.memState.lgRegions = {}; // 次の判定のためにリセット

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
