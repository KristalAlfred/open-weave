// Drive the browser node without a human: launch Chromium with a fake camera,
// open the page against a southbound, and wait for the node to register.
//
//   node check.mjs --southbound http://host:8081 --token <token> [--page URL]
//                  [--serve PORT] [--stay] [--headed] [--video-only] [--node ID]
//   node check.mjs --check-only
//
// --video-only asks the page for the camera alone, for hosts where the fake
// microphone never answers (macOS without microphone permission for the browser).
//
// With --serve the script hosts this directory itself on loopback and opens it
// from http://127.0.0.1:PORT/ (or --page when given, for a different origin).
// With --stay the browser is left running after the node registers, which is how
// the bench keeps a node alive; without it the script exits 0 on success.
//
// --check-only compares the page's copies of the crates/core constants with the
// Rust source and exits, launching no browser and needing no southbound.
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { dirname, extname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const args = parseArgs(process.argv.slice(2));
const checkOnly = Boolean(args["check-only"]);
await checkDeclaredConstants(checkOnly);
if (checkOnly) process.exit(0);

const southbound = args.southbound || process.env.WEAVE_SOUTHBOUND_URL;
const token = args.token ?? process.env.WEAVE_SOUTHBOUND_TOKEN ?? "";
if (!southbound) {
  console.error("usage: check.mjs --southbound URL [--token T] [--page URL] [--serve PORT] [--stay]");
  process.exit(2);
}

const TYPES = { ".html": "text/html", ".js": "text/javascript", ".mjs": "text/javascript" };
let server = null;
if (args.serve) {
  server = createServer(async (req, res) => {
    const file = servedFile(req.url);
    if (!file) {
      res.writeHead(403);
      res.end();
      return;
    }
    try {
      const body = await readFile(file);
      res.writeHead(200, { "content-type": TYPES[extname(file)] || "application/octet-stream" });
      res.end(body);
    } catch {
      res.writeHead(404);
      res.end();
    }
  });
  await new Promise((ready, reject) => {
    server.once("error", reject);
    server.listen(Number(args.serve), "127.0.0.1", ready);
  }).catch(error => {
    console.error(`cannot serve on port ${args.serve}: ${error.message}`);
    process.exit(2);
  });
}
const page = args.page || (server ? `http://127.0.0.1:${args.serve}/` : null);
if (!page) {
  console.error("give --page URL, or --serve PORT to host nodes/browser here");
  process.exit(2);
}

// Imported here rather than at the top so --check-only runs without Playwright
// installed.
const { chromium } = await import("playwright");

// The full Chromium build, not the headless shell: only the former answers
// getUserMedia for the fake devices.
const browser = await chromium.launch({
  channel: "chromium",
  headless: !args.headed,
  args: [
    "--use-fake-device-for-media-stream",
    "--use-fake-ui-for-media-stream",
    "--autoplay-policy=no-user-gesture-required",
  ],
});
const context = await browser.newContext({ permissions: ["camera", "microphone"] });
const tab = await context.newPage();
tab.on("console", message => console.log(`[page] ${message.text()}`));
tab.on("pageerror", error => console.log(`[page error] ${error.message}`));
const media = args["video-only"] ? "&media=video" : "";
const pinned = args.node ? `&node=${encodeURIComponent(args.node)}` : "";
await tab.goto(`${page}#southbound=${encodeURIComponent(southbound)}&token=${encodeURIComponent(token)}${media}${pinned}`);
const nodeId = await tab.locator("#node-id").textContent();
console.log(`page open as ${nodeId}`);

const deadline = Date.now() + 30_000;
let seen = false;
while (Date.now() < deadline) {
  const response = await fetch(`${southbound}/v1/nodes`, {
    headers: token ? { authorization: `Bearer ${token}` } : {},
  }).catch(() => null);
  if (response && response.ok) {
    const nodes = await response.json();
    if (nodes.some(node => node.id === nodeId)) {
      seen = true;
      break;
    }
  }
  await new Promise(resolve => setTimeout(resolve, 1000));
}
if (!seen) {
  console.error(`${nodeId} did not appear in ${southbound}/v1/nodes within 30s`);
  await browser.close();
  process.exit(1);
}
console.log(`${nodeId} registered with ${southbound}`);

if (args.stay) {
  console.log("staying up; hop status:");
  setInterval(async () => {
    const hops = await tab.locator("#hops").innerText().catch(() => "");
    const last = await tab.locator("#log").innerText().catch(() => "");
    console.log(hops.replace(/\n+/g, " | ") || "(no hops)", "::", last.split("\n")[0] || "");
  }, 5000);
  await new Promise(() => {});
} else {
  await browser.close();
  server?.close();
}

// The file under `here` a request addresses, or null when the path leaves the
// directory.
function servedFile(url) {
  let path;
  try {
    path = decodeURIComponent(url.split("?")[0].split("#")[0]);
  } catch {
    return null;
  }
  const file = resolve(here, "." + (path === "/" ? "/index.html" : path));
  return file === here || file.startsWith(here + sep) ? file : null;
}

// The page carries its own copies of two weave_core constants: PROTOCOL_VERSION,
// where a registration declaring the wrong one is refused, and DEVICE_TRANSPORT,
// whose tag the page's device branch matches on. Both are compared with
// crates/core before a browser is launched. A Rust source out of reach is
// reported rather than fatal unless `required`: the bench mounts nodes/browser
// alone.
async function checkDeclaredConstants(required) {
  const DRIFT_CHECKS = [
    {
      what: "protocol version",
      page: [/^const PROTOCOL_VERSION = (\d+);$/m, "const PROTOCOL_VERSION = <n>;"],
      core: [/^pub const PROTOCOL_VERSION: u32 = (\d+);$/m, "pub const PROTOCOL_VERSION: u32 = <n>;"],
    },
    {
      what: "device transport",
      page: [/^const DEVICE_TRANSPORT = "([^"]+)";$/m, 'const DEVICE_TRANSPORT = "<tag>";'],
      core: [/^pub const DEVICE_TRANSPORT: &str = "([^"]+)";$/m, 'pub const DEVICE_TRANSPORT: &str = "<tag>";'],
    },
  ];

  const pagePath = join(here, "node.js");
  const corePath = resolve(here, "..", "..", "crates", "core", "src", "lib.rs");

  const pageSource = await read(pagePath);
  if (pageSource === null) fail(`cannot read ${pagePath}`);
  const coreSource = await read(corePath);

  for (const check of DRIFT_CHECKS) {
    const page = declaredValue(pageSource, check.page[0], pagePath, check.page[1]);
    if (coreSource === null) {
      const message = `node.js declares ${check.what} \`${page}\`, unchecked: cannot read ${corePath}`;
      if (required) fail(message);
      console.warn(message);
      continue;
    }
    const core = declaredValue(coreSource, check.core[0], corePath, check.core[1]);
    if (core !== page) {
      fail(`${check.what} mismatch: ${pagePath} declares \`${page}\`, ${corePath} declares \`${core}\``);
    }
    console.log(`${check.what} \`${page}\` matches ${corePath}`);
  }
}

function read(path) {
  return readFile(path, "utf8").catch(() => null);
}

function declaredValue(source, pattern, path, form) {
  const match = pattern.exec(source);
  if (!match) fail(`${path} does not declare \`${form}\``);
  return match[1];
}

function fail(message) {
  console.error(message);
  process.exit(1);
}

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (!arg.startsWith("--")) continue;
    const key = arg.slice(2);
    const next = argv[i + 1];
    if (next === undefined || next.startsWith("--")) out[key] = true;
    else out[key] = argv[++i];
  }
  return out;
}
