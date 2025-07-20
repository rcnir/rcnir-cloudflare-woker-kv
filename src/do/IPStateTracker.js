/*
 * =================================================================
 * 目次 (Table of Contents)
 * =================================================================
 * 1. エクスポートクラス宣言 (IPStateTracker)
 * 2. fetch: DOエントリポイント
 * 3. getState / putState: ストレージ状態管理
 * 4. incrementCount: 違反カウント処理
 * 5. handleLocaleCheck: ロケールファンアウト検出（修正版）
 * 6. handleRateLimit: 安全Botのレート制限
 * 7. parseLocale: パスから言語・国を抽出（特例含む）
 * =================================================================
 */

export class IPStateTracker {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    // --- 検知ルール設定 ---
    this.LOCALE_WINDOW_MS = 10 * 1000;      // 10秒
    this.RATE_LIMIT_COUNT = 10;            // 10リクエストまで許容
    this.RATE_LIMIT_DURATION_MS = 60 * 1000;  // 1分間
  }

  /*
   * =================================================================
   * 2. fetch: DOエントリポイント-
   * =================================================================
   */
  async fetch(request) {
    const url = new URL(request.url);
    const ip = request.headers.get("CF-Connecting-IP"); // このIPはリクエスト元IPであり、DOのIDとは異なる

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
        // DOのストレージにある全てのキー（IPアドレス）とその状態を取得
        // limit を設定しないと、デフォルトの1000件で打ち切られる場合があるため、明示的に大きな値を設定
        const data = await this.state.storage.list({ limit: 10000 });
        const highCountIps = [];
        for (const [key, state] of data.entries()) {
          // state が存在し、かつ count >= 4 の場合のみ抽出
          if (state && state.count >= 4) {
            highCountIps.push(key); // キー自体がIPアドレス
          }
        }
        return new Response(JSON.stringify(highCountIps), {
          headers: { "Content-Type": "application/json" }
        });
      }
      // --- DOストレージをリセットする管理者用エンドポイント ---
      // !! 認証を追加することを強く推奨します !!
      // 例: `wrangler.toml` で設定した `DO_RESET_KEY` を利用
      // `npx wrangler deploy` 時には `env.DO_RESET_KEY` が注入されます。
      // ただし、このメソッドは個々のDurable Objectインスタンスのストレージをリセットするものです。
      // 全てのIPのDurable Objectインスタンスをリセットしたい場合は、
      // `wrangler.toml` のDurable Objectバインディングを削除・再作成するのが最も確実です。
      case "/admin/reset-all-violations": {
        const resetKey = url.searchParams.get("reset_key"); // URLのクエリパラメータからキーを取得

        // 環境変数 'DO_RESET_KEY' と比較して認証
        if (!this.env.DO_RESET_KEY || resetKey !== this.env.DO_RESET_KEY) {
          return new Response("Unauthorized reset attempt.", { status: 401 });
        }

        await this.state.storage.deleteAll(); // **Durable Objectの永続ストレージを全削除**
        console.log("Durable Object storage for this instance has been reset.");
        return new Response("Durable Object storage reset successfully.", { status: 200 });
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
 * 5. handleLocaleCheck: ロケールファンアウト検出（修正版）
 * =================================================================
 */
async handleLocaleCheck(ip, path) {
  const state = await this.getState(ip);
  const now = Date.now();

  // --- ✅ 除外対象パス (既存) ---
  const excludePatterns = [
    '^/\\.well-known/',          // Shopify Monorail 等
    '^/sf_private_access_tokens', // プライベートトークンAPI
    // ここに必要に応じて追加可能
  ];
  for (const pat of excludePatterns) {
    if (new RegExp(pat).test(path)) {
      return new Response(JSON.stringify({ violation: false }), {
        headers: { "Content-Type": "application/json" }
      });
    }
  }
  // --- 追加: 新たなシステム関連パスの除外パターン ---
  const newExcludePatterns = [
      '^/wpm@', // ShopifyのWeb Pixel Manager関連パス (例: /wpm@aa986369...)
  ];
  for (const pat of newExcludePatterns) {
      if (new RegExp(pat).test(path)) {
          return new Response(JSON.stringify({ violation: false }), {
              headers: { "Content-Type": "application/json" }
          });
      }
  }


  const { lang, country } = parseLocale(path);

  // ★★★ 追加: 'unknown'なロケールは処理対象外とする ★★★
  // `parseLocale` が "unknown" を返した場合、ロケールファンアウトの判定には含めない
  if (lang === "unknown" || country === "unknown") {
      return new Response(JSON.stringify({ violation: false }), {
          headers: { "Content-Type": "application/json" }
      });
  }


  // --- 古い記録の掃除（10秒以上前） ---
  for (const [key, ts] of Object.entries(state.lgRegions)) {
    if (now - ts > this.LOCALE_WINDOW_MS) {
      delete state.lgRegions[key];
    }
  }

  // --- 最新ステートを追加 ---
  const currentKey = `${lang}-${country}`;
  state.lgRegions[currentKey] = now;

  // --- 国セットのチェック（3国以上 = 違反）---
  const countries = new Set(
    Object.keys(state.lgRegions).map(k => k.split("-")[1])
  );
  const violation = countries.size >= 3;

  // --- 違反時処理 ---
  if (violation) {
    state.count += 1;
    state.lgRegions = {};  // リセット
    await this.putState(ip, state);
    return new Response(JSON.stringify({ violation: true, count: state.count }), {
      headers: { "Content-Type": "application/json" }
    });
  }

  // --- 通常時の保存および否違反レスポンス ---
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
  // ".well-known" や "wpm@" のようなパスは、この分岐に到達し "unknown-unknown" を返す。
  // その後、handleLocaleCheck でこれらのパターンは除外されるため、誤検知の原因にはならない。
  return { lang: "unknown", country: "unknown" };
}

