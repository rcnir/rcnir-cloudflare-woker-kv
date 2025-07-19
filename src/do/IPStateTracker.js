/*
 * =================================================================
 * 目次 (Table of Contents)
 * =================================================================
 * 1. エクスポートクラス宣言 (IPStateTracker)
 * 2. fetch: DOエントリポイント
 * 3. getState / putState: ストレージ状態管理
 * 4. incrementCount: 違反カウント処理
 * 5. handleLocaleCheck: ロケールファンアウト検出（新ロジック）
 * 6. handleRateLimit: 安全Botのレート制限
 * 7. parseLocale: パスから言語・国を抽出（特例含む）
 * =================================================================
 */

export class IPStateTracker {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    // --- 検知ルール設定 ---
    this.LOCALE_WINDOW_MS = 10 * 1000;         // 10秒
    this.RATE_LIMIT_COUNT = 10;                // 10リクエストまで許容
    this.RATE_LIMIT_DURATION_MS = 60 * 1000;   // 1分間
  }

  /*
   * =================================================================
   * 2. fetch: DOエントリポイント
   * =================================================================
   */
  async fetch(request) {
    const url = new URL(request.url);
    const ip = request.headers.get("CF-Connecting-IP");

    switch (url.pathname) {
      case "/check-locale": {
        const { path } = await request.json(); // localeではなくpathを受信
        return this.handleLocaleCheck(ip, path);
      }
      case "/trigger-violation": {
        return this.incrementCount(ip);
      }
      case "/rate-limit": {
        return this.handleRateLimit(ip);
      }
      case "/list-high-count": {
        const data = await this.state.storage.list({ limit: 1000 });
        const highCountIps = [];
        for (const [ip, state] of data.entries()) {
          if (state && state.count >= 4) {
            highCountIps.push(ip);
          }
        }
        return new Response(JSON.stringify(highCountIps), {
          headers: { "Content-Type": "application/json" }
        });
      }
      default:
        return new Response("Not found", { status: 404 });
    }
  }

  /*
   * =================================================================
   * 3. getState / putState: ストレージ状態管理
   * =================================================================
   */
  async getState(ip) {
    let state = await this.state.storage.get(ip) || {
      count: 0,
      lgRegions: {},
      rateLimit: { count: 0, firstRequest: 0 }
    };
    if (typeof state.lgRegions === "undefined") {
      state.lgRegions = {};
    }
    if (typeof state.rateLimit === "undefined") {
      state.rateLimit = { count: 0, firstRequest: 0 };
    }
    return state;
  }

  async putState(ip, state) {
    await this.state.storage.put(ip, state);
  }

  /*
   * =================================================================
   * 4. incrementCount: 違反カウント処理
   * =================================================================
   */
  async incrementCount(ip) {
    const state = await this.getState(ip);
    state.count += 1;
    await this.putState(ip, state);
    return new Response(JSON.stringify({ count: state.count }), {
      headers: { "Content-Type": "application/json" }
    });
  }

  /*
   * =================================================================
   * 5. handleLocaleCheck: ロケールファンアウト検出（新ロジック）
   * =================================================================
   */
async handleLocaleCheck(ip, path) {
  const state = await this.getState(ip);
  const now = Date.now();

  const { lang, country } = parseLocale(path);

  // 古い記録を掃除
  for (const [key, ts] of Object.entries(state.lgRegions)) {
    if (now - ts > this.LOCALE_WINDOW_MS) {
      delete state.lgRegions[key];
    }
  }

  state.lgRegions[`${lang}-${country}`] = now;

  // 国セットのサイズをチェック
  const countries = new Set(
    Object.keys(state.lgRegions).map(k => k.split("-")[1])
  );

  const violation = countries.size >= 2;

  if (violation) {
    state.count += 1;
    state.lgRegions = {};
    await this.putState(ip, state);
    return new Response(JSON.stringify({ violation: true, count: state.count }), {
      headers: { "Content-Type": "application/json" }
    });
  }

  await this.putState(ip, state);
  return new Response(JSON.stringify({ violation: false }), {
    headers: { "Content-Type": "application/json" }
  });
}

  /*
   * =================================================================
   * 6. handleRateLimit: 安全Botのレート制限
   * =================================================================
   */
  async handleRateLimit(ip) {
    const state = await this.getState(ip);
    const now = Date.now();

    if (now - state.rateLimit.firstRequest > this.RATE_LIMIT_DURATION_MS) {
      state.rateLimit = { count: 1, firstRequest: now };
    } else {
      state.rateLimit.count++;
    }

    await this.putState(ip, state);

    const allowed = state.rateLimit.count <= this.RATE_LIMIT_COUNT;
    return new Response(JSON.stringify({ allowed }), {
      headers: { "Content-Type": "application/json" }
    });
  }
}

/*
 * =================================================================
 * 7. parseLocale: パスから言語・国を抽出（特例含む）
 * =================================================================
 */
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
