import html from "./index.html";
// BTCUSDT research collector: a Durable Object keeps Binance WebSockets open 24/7.
const BASE = "wss://fstream.binance.com";
const MARKET = BASE + "/market/stream?streams=btcusdt@markPrice@1s/btcusdt@aggTrade/btcusdt@ticker";
const PUBLIC = BASE + "/public/ws/btcusdt@depth20@500ms";
const json = (o) => new Response(JSON.stringify(o), { headers: { "content-type": "application/json", "cache-control": "no-store" } });

const stub = (env) => env.COLLECTOR.get(env.COLLECTOR.idFromName("btcusdt"), { locationHint: "apac" });

export default {
  async scheduled(event, env, ctx) {   // cron every minute: starts/keeps the collector alive even with no visitors
    ctx.waitUntil(stub(env).fetch("https://do/api/state"));
  },
  async fetch(req, env) {
    const u = new URL(req.url);
    if (u.pathname === "/api/state") {
      return stub(env).fetch(req);
    }
    return new Response(html, { headers: { "content-type": "text/html;charset=utf-8", "cache-control": "no-store" } });
  },
};

export class Collector {
  constructor(state) {
    this.st = state;
    this.ws = {}; this.opening = {}; this.err = {};
    this.price = 0; this.priceTs = 0; this.change = null; this.bids = []; this.asks = []; this.depthTs = 0;
    this.q = []; this.buy = 0; this.sell = 0; this.sig = "WAIT"; this.log = [];
    state.blockConcurrencyWhile(async () => { this.log = (await state.storage.get("log")) || []; });
  }

  async fetch() {
    await this.ensure();
    if (!(await this.st.storage.getAlarm())) await this.st.storage.setAlarm(Date.now() + 30000);
    return json(this.snapshot());
  }

  async alarm() {            // every 30s: reconnect if a stream dropped
    await this.ensure();
    await this.st.storage.setAlarm(Date.now() + 30000);
  }

  async ensure() {
    await Promise.all([this.open("market", MARKET, (m) => this.onMarket(m)), this.open("depth", PUBLIC, (d) => this.onDepth(d))]);
  }

  async open(key, url, onMsg) {
    if (this.ws[key] || this.opening[key]) return;
    this.opening[key] = true;
    try {
      const r = await fetch(url.replace("wss://", "https://"), { headers: { Upgrade: "websocket" } });
      const ws = r.webSocket;
      if (!ws) { this.err[key] = "HTTP " + r.status; return; }
      ws.accept(); this.ws[key] = ws; this.err[key] = null;
      ws.addEventListener("message", (e) => { try { onMsg(JSON.parse(e.data)); } catch {} });
      const drop = () => { if (this.ws[key] === ws) this.ws[key] = null; };
      ws.addEventListener("close", drop); ws.addEventListener("error", drop);
    } catch (e) { this.err[key] = String(e).slice(0, 120); }
    finally { this.opening[key] = false; }
  }

  onMarket(m) {
    const s = m.stream || "", d = m.data;
    if (s.includes("markPrice")) { this.price = +d.p; this.priceTs = Date.now(); this.signal(); }
    else if (s.includes("aggTrade")) {
      const n = +d.p * +d.q, buy = !d.m, now = Date.now();
      this.q.push({ t: now, n, buy }); buy ? (this.buy += n) : (this.sell += n);
      while (this.q.length && this.q[0].t < now - 60000) { const o = this.q.shift(); o.buy ? (this.buy -= o.n) : (this.sell -= o.n); }
    } else if (s.includes("ticker")) this.change = +d.P;
  }

  onDepth(d) { this.bids = d.b || []; this.asks = d.a || []; this.depthTs = Date.now(); }

  imb(n) {
    const f = (a) => a.slice(0, n).reduce((s, x) => s + +x[0] * +x[1], 0);
    const b = f(this.bids), a = f(this.asks);
    return (b - a) / (b + a || 1);
  }

  signal() {
    if (!this.price || !this.bids.length || !this.asks.length) return;
    const im = this.imb(10), fl = (this.buy - this.sell) / (this.buy + this.sell || 1);
    let s = "WAIT", r = "Confirmation incomplete; wait.";
    if (im > 0.18 && fl > 0.15) { s = "LONG"; r = "Bid liquidity and aggressive buying align; structure not validated."; }
    if (im < -0.18 && fl < -0.15) { s = "SHORT"; r = "Ask liquidity and aggressive selling align; structure not validated."; }
    this.reason = r;
    if (s !== this.sig) {
      this.sig = s;
      const dist = Math.max((+this.asks[0][0] - +this.bids[0][0]) * 3, this.price * 0.002);
      this.log.push({ t: Date.now(), s, p: this.price, sl: s === "LONG" ? this.price - dist : s === "SHORT" ? this.price + dist : null, tp: s === "LONG" ? this.price + dist * 1.5 : s === "SHORT" ? this.price - dist * 1.5 : null });
      this.log = this.log.slice(-200);
      this.st.storage.put("log", this.log);
    }
  }

  snapshot() {
    const now = Date.now(), L = this.log.at(-1);
    const live = this.sig !== "WAIT" && L;
    return {
      now, price: this.price, age: this.priceTs ? (now - this.priceTs) / 1000 : null,
      change: this.change, imb20: this.imb(20), flow: (this.buy - this.sell) / (this.buy + this.sell || 1),
      signal: this.sig, reason: this.reason || "Collecting data…",
      entry: live ? L.p : this.price, sl: live ? L.sl : null, tp: live ? L.tp : null,
      bids: this.bids.slice(0, 8), asks: this.asks.slice(0, 8),
      up: { market: !!this.ws.market, depth: !!this.ws.depth }, err: this.err,
      log: this.log.slice(-15).reverse(),
    };
  }
}
