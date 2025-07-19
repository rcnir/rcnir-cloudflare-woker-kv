// src/do/IPBlockCounter.js
export class IPBlockCounter {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }

  async fetch(request) {
    const url = new URL(request.url);
    
    // Cron Triggerからのリクエストを処理
    if (url.pathname === "/list-high-count") {
      const storedData = await this.state.storage.list({ limit: 1000 });
      const highCountIps = [];
      for (const [ip, count] of storedData.entries()) {
        if (count >= 3) {
          highCountIps.push(ip);
        }
      }
      return new Response(JSON.stringify(highCountIps), {
        headers: { "Content-Type": "application/json" },
      });
    }

    // 通常のカウントアップリクエスト
    const { ip } = await request.json();
    if (!ip) {
      return new Response("Bad Request: IP is required.", { status: 400 });
    }
    const currentCount = (await this.state.storage.get(ip)) || 0;
    const newCount = currentCount + 1;
    await this.state.storage.put(ip, newCount);

    return new Response(
      JSON.stringify({ ip, count: newCount }),
      { headers: { "Content-Type": "application/json" } }
    );
  }
}
