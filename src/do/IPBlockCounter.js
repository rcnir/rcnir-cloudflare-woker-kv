// src/do/IPBlockCounter.js
export class IPBlockCounter {
  constructor(ctx, env) {
    this.state = ctx.storage;
    this.env = env;
  }

  async fetch(request) {
    const { ip } = await request.json();
    const current = (await this.state.get(ip)) || 0;
    const next = current + 1;
    await this.state.put(ip, next);
    return new Response(
      JSON.stringify({ ip, count: next }),
      { headers: { "Content-Type": "application/json" } }
    );
  }
}
