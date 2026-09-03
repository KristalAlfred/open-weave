"use strict";

// Must equal weave_core::PROTOCOL_VERSION; check.mjs reads this line and
// compares it with the constant in crates/core.
const PROTOCOL_VERSION = 2;
const API_V1 = "/v1";
const HEARTBEAT_MS = 5000;
const POLL_MS = 2000;
const STALL_POLLS = 3;
const RETRY_MS = 5000;
const FAILED_AFTER_ATTEMPTS = 5;
const ICE_GATHER_MS = 2000;
const CAPTURE_MS = 4000;
const NODE_ID_KEY = "weave-node-id";

const config = readConfig();
const nodeId = readNodeId();
const hops = new Map();
let registered = false;
let fatal = null;

function readConfig() {
  const params = new URLSearchParams(location.hash.slice(1));
  return {
    southbound: (params.get("southbound") || "").replace(/\/+$/, ""),
    token: params.get("token") || "",
    media: params.get("media") === "video" ? { video: true } : { video: true, audio: true },
    node: params.get("node") || "",
  };
}

// `#node=<id>` pins the id, for a page that must keep its name across restarts.
// Otherwise one id per tab for as long as it lives: a reload keeps the node, a
// new tab is a new node.
function readNodeId() {
  if (config.node) return config.node;
  let id = sessionStorage.getItem(NODE_ID_KEY);
  if (!id) {
    const bytes = crypto.getRandomValues(new Uint8Array(4));
    id = "browser-" + Array.from(bytes, b => b.toString(16).padStart(2, "0")).join("");
    sessionStorage.setItem(NODE_ID_KEY, id);
  }
  return id;
}

// --- southbound ---

async function southbound(method, path, body) {
  const headers = {};
  if (config.token) headers.authorization = `Bearer ${config.token}`;
  if (body !== undefined) headers["content-type"] = "application/json";
  return fetch(`${config.southbound}${API_V1}${path}`, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

function registration() {
  return {
    protocol_version: PROTOCOL_VERSION,
    node: {
      id: nodeId,
      endpoint: `browser://${nodeId}`,
      status: "ready",
      capabilities: {
        adapters: [],
        transports: [
          { name: "whip", roles: ["connect"] },
          { name: "whep", roles: ["connect"] },
        ],
        devices: ["capture", "display"],
        data_plane: { default: { host: "browser", reachability: "outbound_only" } },
        relay: false,
      },
    },
    endpoints: [],
    hop_status: hopStatuses(),
  };
}

async function register() {
  const response = await southbound("POST", "/nodes/register", registration());
  if (response.status === 409) {
    fatal = `southbound refused protocol version ${PROTOCOL_VERSION}: ${await response.text()}`;
    throw new Error(fatal);
  }
  if (!response.ok) throw new Error(`register: ${response.status} ${await response.text()}`);
  registered = true;
}

async function heartbeat() {
  if (fatal) return;
  try {
    if (!registered) {
      await register();
      return;
    }
    const response = await southbound("POST", `/nodes/${nodeId}/heartbeat`, {
      node_id: nodeId,
      status: "ready",
      endpoints: [],
      hop_status: hopStatuses(),
    });
    if (response.status === 404) registered = false;
    else if (!response.ok) throw new Error(`heartbeat: ${response.status}`);
  } catch (error) {
    registered = false;
    log(`southbound: ${error.message}`);
  } finally {
    render();
  }
}

async function pollDesired() {
  if (fatal || !registered) return;
  try {
    const response = await southbound("GET", `/nodes/${nodeId}/desired`);
    if (!response.ok) throw new Error(`desired: ${response.status}`);
    reconcile(await response.json());
  } catch (error) {
    log(`desired: ${error.message}`);
  }
  for (const hop of hops.values()) await hop.observe();
  render();
}

// Hops are matched by id. A hop whose spec changed is torn down and restarted;
// an unchanged one is left alone, whatever its state.
function reconcile(desired) {
  const wanted = new Map(desired.map(spec => [spec.id, spec]));
  for (const [id, hop] of hops) {
    const spec = wanted.get(id);
    if (!spec || JSON.stringify(spec) !== JSON.stringify(hop.spec)) {
      hop.stop();
      hops.delete(id);
    }
  }
  for (const [id, spec] of wanted) {
    if (hops.has(id)) continue;
    const hop = Hop.from(spec);
    hops.set(id, hop);
    hop.start();
  }
}

function hopStatuses() {
  return Array.from(hops.values(), hop => hop.status());
}

// --- byte progress ---

// Byte progress across polls is what separates a connected session that is
// carrying media from one that is silent; a counter that stops moving after
// having moved is a stall.
class Progress {
  constructor() {
    this.last = null;
    this.lastAt = 0;
    this.stale = 0;
    this.ever = false;
    this.advanced = false;
    this.rateMbps = 0;
  }

  observe(bytes) {
    const now = performance.now();
    if (bytes == null) return;
    if (this.last != null) {
      if (bytes > this.last) {
        this.ever = true;
        this.stale = 0;
        this.advanced = true;
        const seconds = (now - this.lastAt) / 1000;
        this.rateMbps = seconds > 0 ? ((bytes - this.last) * 8) / seconds / 1e6 : 0;
      } else {
        this.stale += 1;
        this.advanced = false;
        this.rateMbps = 0;
      }
    }
    this.last = bytes;
    this.lastAt = now;
  }

  condition(connected) {
    if (!connected) return "connecting";
    if (this.ever && this.stale >= STALL_POLLS) return "stalled";
    if (this.advanced) return "flowing";
    return "connected";
  }
}

// --- hops ---

// Must equal weave_core::DEVICE_TRANSPORT; check.mjs reads this line and
// compares it with the constant in crates/core.
const DEVICE_TRANSPORT = "device";

// A socket named as core prints one: `srt`, `whip`, `whep`, `capture device`,
// `display device`.
function socketName(socket) {
  if (!socket) return "nothing";
  return socket.transport === DEVICE_TRANSPORT ? `${socket.role} device` : socket.transport;
}

class Hop {
  static from(spec) {
    const egress = spec.egresses[0];
    if (spec.ingress.transport === DEVICE_TRANSPORT && egress && egress.transport === "whip") {
      return new SenderHop(spec);
    }
    if (spec.ingress.transport === "whep" && egress && egress.transport === DEVICE_TRANSPORT) {
      return new ReceiverHop(spec);
    }
    return new UnsupportedHop(spec);
  }

  constructor(spec) {
    this.spec = spec;
    this.pc = null;
    this.resource = null;
    this.error = null;
    this.attempts = 0;
    this.retry = null;
    this.stopped = false;
    this.progress = new Progress();
    this.stats = null;
    this.codecs = [];
    this.connected = false;
  }

  async start() {
    try {
      await this.open();
      this.error = null;
      this.attempts = 0;
    } catch (error) {
      this.attempts += 1;
      this.error = error.message;
      log(`${this.spec.id}: ${error.message}`);
      this.close();
      if (!this.stopped) this.retry = setTimeout(() => this.start(), RETRY_MS);
    }
    render();
  }

  stop() {
    this.stopped = true;
    clearTimeout(this.retry);
    this.close();
  }

  close() {
    if (this.resource) {
      fetch(this.resource, { method: "DELETE" }).catch(() => {});
      this.resource = null;
    }
    if (this.pc) {
      this.pc.close();
      this.pc = null;
    }
    this.connected = false;
  }

  newPeerConnection() {
    const pc = new RTCPeerConnection();
    pc.onconnectionstatechange = () => {
      if (["failed", "disconnected", "closed"].includes(pc.connectionState) && this.pc === pc) {
        log(`${this.spec.id}: connection ${pc.connectionState}; reconnecting`);
        this.close();
        if (!this.stopped) this.retry = setTimeout(() => this.start(), RETRY_MS);
      }
    };
    this.pc = pc;
    return pc;
  }

  // WHIP and WHEP share one signalling exchange: POST the offer as SDP, read the
  // answer, remember the session resource from Location for teardown. Strom does
  // not expose Location to a cross-origin page, so the resource may stay unknown;
  // closing the peer connection then relies on Strom's inactivity timeout.
  async signal(url) {
    const pc = this.pc;
    await pc.setLocalDescription(await pc.createOffer());
    await iceGathered(pc, ICE_GATHER_MS);
    const response = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/sdp" },
      body: pc.localDescription.sdp,
    });
    if (!response.ok) throw new Error(`${url} → ${response.status} ${(await response.text()).trim()}`);
    const answer = await response.text();
    const location = response.headers.get("location");
    this.resource = location ? new URL(location, url).href : null;
    if (!this.resource) log(`${this.spec.id}: session created; Location not exposed to this origin, teardown relies on the server's timeout`);
    if (this.pc !== pc) return;
    await pc.setRemoteDescription({ type: "answer", sdp: answer });
  }

  state() {
    if (this.error) return this.attempts >= FAILED_AFTER_ATTEMPTS ? "failed" : "pending";
    return this.pc ? "provisioned" : "pending";
  }

  async observe() {
    if (!this.pc) {
      this.connected = false;
      this.stats = null;
      return;
    }
    this.connected = this.pc.connectionState === "connected";
    const report = await this.pc.getStats();
    const totals = { bytesSent: 0, bytesReceived: 0, packetsLost: 0, sentLost: 0, retransmittedSent: 0, retransmittedReceived: 0 };
    const codecs = new Map();
    for (const entry of report.values()) {
      if (entry.type === "codec") codecs.set(entry.id, entry.mimeType);
    }
    this.codecs = [];
    for (const entry of report.values()) {
      if (entry.type === "outbound-rtp") {
        totals.bytesSent += entry.bytesSent || 0;
        totals.retransmittedSent += entry.retransmittedPacketsSent || 0;
        this.codecs.push(`${codecs.get(entry.codecId) || entry.kind}↑${entry.bytesSent || 0}`);
      } else if (entry.type === "remote-inbound-rtp") {
        totals.sentLost += entry.packetsLost || 0;
      } else if (entry.type === "inbound-rtp") {
        totals.bytesReceived += entry.bytesReceived || 0;
        totals.packetsLost += entry.packetsLost || 0;
        totals.retransmittedReceived += entry.retransmittedPacketsReceived || 0;
        this.codecs.push(`${codecs.get(entry.codecId) || entry.kind}↓${entry.bytesReceived || 0}`);
      }
    }
    this.progress.observe(this.sending ? totals.bytesSent : totals.bytesReceived);
    this.stats = {
      connections: this.connected ? 1 : 0,
      ingress_rate_mbps: this.sending ? 0 : this.progress.rateMbps,
      egress_rate_mbps: this.sending ? this.progress.rateMbps : 0,
      packets_sent_lost: totals.sentLost,
      packets_retransmitted: totals.retransmittedSent,
      packets_received_lost: totals.packetsLost,
      packets_received_retransmitted: totals.retransmittedReceived,
    };
  }

  status() {
    const status = {
      id: this.spec.id,
      node_id: nodeId,
      state: this.state(),
      ingress: this.ingressCondition(),
      egress: this.egressCondition(),
    };
    if (this.stats) status.stats = this.stats;
    return status;
  }
}

// device (camera) → whip connect
class SenderHop extends Hop {
  constructor(spec) {
    super(spec);
    this.sending = true;
    this.stream = null;
  }

  async open() {
    if (!this.stream) this.stream = await captureDevices();
    const pc = this.newPeerConnection();
    for (const track of this.stream.getTracks()) {
      pc.addTransceiver(track, { direction: "sendonly" });
    }
    await this.signal(this.spec.egresses[0].url);
  }

  stop() {
    super.stop();
    if (this.stream) {
      for (const track of this.stream.getTracks()) track.stop();
      this.stream = null;
    }
  }

  // The camera is this hop's ingress: producing while its tracks are live.
  ingressCondition() {
    const tracks = this.stream ? this.stream.getTracks() : [];
    if (!tracks.some(t => t.readyState === "live")) return "idle";
    return tracks.some(t => !t.muted) ? "flowing" : "connected";
  }

  egressCondition() {
    return this.progress.condition(this.connected);
  }
}

// whep connect → device (screen)
class ReceiverHop extends Hop {
  constructor(spec) {
    super(spec);
    this.sending = false;
    this.video = document.createElement("video");
    this.video.autoplay = true;
    this.video.playsInline = true;
    this.video.muted = true;
    this.video.controls = true;
    this.figure = el("figure");
    this.figure.append(this.video, el("figcaption", null, spec.id));
  }

  async open() {
    const pc = this.newPeerConnection();
    pc.addTransceiver("video", { direction: "recvonly" });
    pc.addTransceiver("audio", { direction: "recvonly" });
    const stream = new MediaStream();
    pc.ontrack = event => {
      stream.addTrack(event.track);
      this.video.srcObject = stream;
      this.video.play().catch(() => {});
    };
    await this.signal(this.spec.ingress.url);
  }

  stop() {
    super.stop();
    this.video.srcObject = null;
    this.figure.remove();
  }

  ingressCondition() {
    return this.progress.condition(this.connected);
  }

  // The screen is this hop's egress: consuming once the element plays.
  egressCondition() {
    if (!this.video.srcObject) return "connecting";
    return this.video.readyState >= 2 && !this.video.paused ? "flowing" : "connected";
  }
}

class UnsupportedHop extends Hop {
  async start() {
    this.error = `this node realises device→whip and whep→device only, not ${socketName(this.spec.ingress)}→${socketName(this.spec.egresses[0])}`;
    log(`${this.spec.id}: ${this.error}`);
    render();
  }

  state() {
    return "failed";
  }

  ingressCondition() {
    return "idle";
  }

  egressCondition() {
    return "idle";
  }
}

// Camera and microphone, or the camera alone with `#media=video`. A device the
// OS never answers for leaves getUserMedia pending rather than rejecting, and a
// pending request blocks every later one in the page, so the wait is bounded
// and there is no in-page fallback: pick the devices in the URL.
function captureDevices() {
  if (!navigator.mediaDevices) {
    return Promise.reject(
      new Error("no camera access: the page must be served from a secure context (https, or http on localhost)"),
    );
  }
  const constraints = config.media;
  return Promise.race([
    navigator.mediaDevices.getUserMedia(constraints),
    new Promise((_, reject) =>
      setTimeout(
        () => reject(new Error(`getUserMedia(${JSON.stringify(constraints)}) did not answer within ${CAPTURE_MS} ms`)),
        CAPTURE_MS,
      ),
    ),
  ]);
}

function iceGathered(pc, timeoutMs) {
  if (pc.iceGatheringState === "complete") return Promise.resolve();
  return new Promise(resolve => {
    const done = () => {
      pc.removeEventListener("icegatheringstatechange", check);
      resolve();
    };
    const check = () => {
      if (pc.iceGatheringState === "complete") done();
    };
    pc.addEventListener("icegatheringstatechange", check);
    setTimeout(done, timeoutMs);
  });
}

// --- page ---

const logLines = [];
function log(line) {
  logLines.unshift(`${new Date().toLocaleTimeString()} ${line}`);
  logLines.length = Math.min(logLines.length, 20);
}

function manifests() {
  return `# camera to a Strom node
name: ${nodeId}-cam
source:
  device:
    node: ${nodeId}
destinations:
  - srt:
      node: strom-node-1

# a Strom node's SRT ingress to this screen
name: ${nodeId}-return
source:
  srt:
    node: strom-node-1
destinations:
  - device:
      node: ${nodeId}`;
}

function el(tag, cls, text) {
  const node = document.createElement(tag);
  if (cls) node.className = cls;
  if (text != null) node.textContent = text;
  return node;
}

function render() {
  document.getElementById("node-id").textContent = nodeId;
  document.getElementById("southbound").textContent = config.southbound || "(no #southbound= in the URL)";
  const state = document.getElementById("registration");
  state.textContent = fatal ? "rejected" : registered ? "registered" : "registering";
  state.className = `pill ${fatal ? "bad" : registered ? "good" : "wait"}`;
  document.getElementById("fatal").textContent = fatal || "";
  document.getElementById("manifests").textContent = manifests();

  const rows = document.getElementById("hops");
  rows.replaceChildren();
  if (hops.size === 0) rows.append(el("div", "empty", "no hops desired for this node"));
  for (const hop of hops.values()) {
    const status = hop.status();
    const row = el("div", "hop");
    row.append(
      el("span", "id", hop.spec.id),
      el("span", `pill ${status.state}`, status.state),
      el("span", "cond", `${hop.spec.ingress.transport} ${status.ingress}`),
      el("span", "arrow", "→"),
      el("span", "cond", `${(hop.spec.egresses[0] || {}).transport || "?"} ${status.egress}`),
    );
    if (hop.stream) {
      row.append(el("span", "cond", hop.stream.getTracks().map(t => t.kind).join("+")));
    }
    if (status.stats) {
      const rate = hop.sending ? status.stats.egress_rate_mbps : status.stats.ingress_rate_mbps;
      row.append(el("span", "rate", `${rate.toFixed(2)} Mb/s`));
      if (hop.codecs && hop.codecs.length) row.append(el("span", "rate", hop.codecs.join(" ")));
    }
    if (hop.error) row.append(el("span", "err", hop.error));
    rows.append(row);
  }

  const videos = document.getElementById("videos");
  for (const hop of hops.values()) {
    if (hop instanceof ReceiverHop && !hop.figure.isConnected) {
      videos.append(hop.figure);
    }
  }
  document.getElementById("log").textContent = logLines.join("\n");
}

document.getElementById("copy").addEventListener("click", () => {
  navigator.clipboard.writeText(manifests()).catch(() => {});
});

render();
if (!config.southbound) {
  fatal = "open this page as index.html#southbound=http://host:8081&token=…";
  render();
} else {
  heartbeat();
  setInterval(heartbeat, HEARTBEAT_MS);
  setInterval(pollDesired, POLL_MS);
}
