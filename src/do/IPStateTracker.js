// src/do/IPStateTracker.js

export class IPStateTracker {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.LOCALE_WINDOW_MS = 30 * 1000; // 30秒
    this.LOCALE_THRESHOLD = 3;        // 3ロケール
  }

  async fetch(request) {
    const url = new URL(request.url);
    const ip = request.headers.get("CF-Connecting-IP");

    // 内部APIのルーティング
    switch (url.pathname) {
      case "/check-locale": {
        const { locale } = await request.json();
        return this.handleLocaleCheck(ip, locale);
      }
      case "/trigger-violation": {
        // 偽装やUAパターンなど、即時違反としてカウントする場合
        return this.incrementCount(ip);
      }
      case "/list-high-count": {
        // Cronからのリクエスト
        const data = await this.state.storage.list({ limit: 1000 });
        const highCountIps = [];
        for (const [ip, state] of data.entries()) {
          if (state.count >= 3) {
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
    const state = await this.state.storage.get(ip) || { count: 0, locales: {} };
    // 念のため、古いデータ構造にも対応
    if (typeof state !== 'object' || state === null) {
      return { count: state || 0, locales: {} };
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

    // 古いロケール履歴を削除
    for (const [loc, ts] of Object.entries(state.locales)) {
      if (now - ts > this.LOCALE_WINDOW_MS) {
        delete state.locales[loc];
      }
    }

    // 新しいロケールを追加
    state.locales[locale] = now;

    // 閾値を超えたかチェック
    if (Object.keys(state.locales).length >= this.LOCALE_THRESHOLD) {
      state.count += 1;       // 違反カウントを増やす
      state.locales = {};     // 違反が確定したら履歴をリセット
      await this.putState(ip, state);
      return new Response(JSON.stringify({ violation: true, count: state.count }), { headers: { "Content-Type": "application/json" } });
    }

    // 違反していない場合は、現在の状態を保存するだけ
    await this.putState(ip, state);
    return new Response(JSON.stringify({ violation: false }), { headers: { "Content-Type": "application/json" } });
  }
}
