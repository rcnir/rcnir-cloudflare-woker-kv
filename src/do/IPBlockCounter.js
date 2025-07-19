export class IPBlockCounter {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }

  async fetch(request) {
    const url = new URL(request.url);
    const ip = url.searchParams.get("ip");
    if (!ip) return new Response("Missing IP", { status: 400 });

    const stored = await this.state.storage.get(ip) || 0;
    const newCount = stored + 1;
    await this.state.storage.put(ip, newCount);

    return new Response(JSON.stringify({ ip, count: newCount }), {
      headers: { "Content-Type": "application/json" },
    });
  }
}
