// src/do/IPStateTracker.js

export class IPStateTracker {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    // --- 検知ルールの設定 ---
    this.LOCALE_WINDOW_MS = 10 * 1000;      // 10秒
    this.LOCALE_THRESHOLD = 3;              // 3ロケール
    // --- レート制限ルールの設定 ---
    this.RATE_LIMIT_COUNT = 10;             // 10リクエスト
    this.RATE_LIMIT_DURATION_MS = 60 * 1000; // 1分 (60秒)
  }

  async fetch(request) {
    const url = new URL(request.url);
    const ip = request.headers.get("CF-Connecting-IP");

    switch (url.pathname) {
      case "/check-locale": {
        const { locale } = await request.json();
        return this.handleLocaleCheck(ip, locale);
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
          // 4回以上の違反で永久ブロック対象
          if (state && state.count >= 4) {
            highCountIps.push(ip);
          }
        }
        return new Response(JSON.stringify(highCountIps), { headers: { "Content-Type": "application/json" } });
      }
      default:
        return new Response("Not found", { status: 404 });
    }
  }

  // IPの状態を取得または初期化
  async getState(ip) {
    // ストレージから状態を取得、なければデフォルト値を設定
    let state = await this.state.storage.get(ip) || {
      count: 0,
      locales: {},
      rateLimit: { count: 0, firstRequest: 0 }
    };
    // 古いデータ構造からの移行を考慮
    if (typeof state.locales === 'undefined') {
      state.locales = {};
    }
    if (typeof state.rateLimit === 'undefined') {
      state.rateLimit = { count: 0, firstRequest: 0 };
    }
    return state;
  }
  
  // 状態を保存
  async putState(ip, state) {
    await this.state.storage.put(ip, state);
  }

  // 違反カウントを1増やす
  async incrementCount(ip) {
    const state = await this.getState(ip);
    state.count += 1;
    await this.putState(ip, state);
    return new Response(JSON.stringify({ count: state.count }), { headers: { "Content-Type": "application/json" } });
  }

  // ロケールファンアウトをチェックし、違反ならカウントを増やす
  async handleLocaleCheck(ip, locale) {
    const state = await this.getState(ip);
    const now = Date.now();

    for (const [loc, ts] of Object.entries(state.locales)) {
      if (now - ts > this.LOCALE_WINDOW_MS) {
        delete state.locales[loc];
      }
    }

    state.locales[locale] = now;

    if (Object.keys(state.locales).length >= this.LOCALE_THRESHOLD) {
      state.count += 1;
      state.locales = {};
      await this.putState(ip, state);
      return new Response(JSON.stringify({ violation: true, count: state.count }), { headers: { "Content-Type": "application/json" } });
    }

    await this.putState(ip, state);
    return new Response(JSON.stringify({ violation: false }), { headers: { "Content-Type": "application/json" } });
  }

  // レート制限を処理する関数
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
    
    return new Response(JSON.stringify({ allowed }), { headers: { "Content-Type": "application/json" } });
  }
}
