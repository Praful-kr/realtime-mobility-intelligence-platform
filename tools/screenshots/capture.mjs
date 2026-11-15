// Captures the README screenshots from the RUNNING stack into docs/screenshots/.
//   cd tools/screenshots && npm install
//   node capture.mjs                       # everything
//   node capture.mjs --only a,b            # selected shots (names below)
// Uses the locally installed Google Chrome (no browser download). Grafana login: admin/admin (dev default).
// Env: PIPELINE_WINDOW_JSON={"from_ms":..,"to_ms":..} to show a past live run on the pipeline dashboard.
import { chromium } from "playwright-core";
import { mkdirSync, readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const OUT = join(here, "..", "..", "docs", "screenshots");
mkdirSync(OUT, { recursive: true });
const CHROME = process.env.CHROME_PATH || "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
const GRAFANA = process.env.GRAFANA_URL || "http://localhost:3000";
const arg = (n) => { const i = process.argv.indexOf(n); return i > -1 ? process.argv[i + 1] : undefined; };
const only = arg("--only")?.split(",");

let pipelineRange = "from=now-15m&to=now";
const winFile = process.env.PIPELINE_WINDOW_JSON;
if (winFile && existsSync(winFile)) { const w = JSON.parse(readFileSync(winFile, "utf8")); pipelineRange = `from=${w.from_ms}&to=${w.to_ms}`; }

// h = viewport/clip height (trimmed to the content so there is no dead space). grafana:true => dashboard (kiosk).
const shots = [
  { name: "grafana-pipeline-overview", grafana: true, w: 1700, h: 2010, settle: 9000,
    url: `/d/mobility-pipeline/pipeline-overview?orgId=1&${pipelineRange}&kiosk` },
  { name: "grafana-mobility-demand", grafana: true, w: 1700, h: 1640, settle: 8000,
    url: `/d/mobility-business/mobility-demand-gold?orgId=1&from=2024-01-01T00:00:00.000Z&to=2024-01-04T16:30:00.000Z&kiosk` },
  { name: "grafana-api", grafana: true, w: 1700, h: 880, settle: 7000,
    url: `/d/mobility-api/api?orgId=1&from=now-6m&to=now&kiosk` },
  { name: "prometheus-targets", url: "http://localhost:9090/targets", w: 1500, h: 1390, settle: 3000, clip: true },
  { name: "prometheus-alerts", url: "http://localhost:9090/alerts", w: 1500, h: 600, settle: 3000, clip: true },
  { name: "prometheus-alert-firing", url: "http://localhost:9090/alerts", w: 1500, h: 830, settle: 3000, clip: true,
    click: "TargetDown" },
  { name: "kafka-ui-topics", url: "http://localhost:8080/ui/clusters/mobility/all-topics", w: 1500, h: 480, settle: 4000, clip: true },
  { name: "kafka-ui-brokers", url: "http://localhost:8080/ui/clusters/mobility/brokers", w: 1500, h: 440, settle: 4000, clip: true },
  { name: "kafka-ui-schema-registry", url: "http://localhost:8080/ui/clusters/mobility/schemas", w: 1500, h: 300, settle: 4000, clip: true },
  { name: "api-swagger-docs", url: "http://localhost:8000/docs", w: 1400, h: 1160, settle: 3000, clip: true },
];

const browser = await chromium.launch({ executablePath: CHROME, headless: true });
const ctx = await browser.newContext({ deviceScaleFactor: 1 });
// Log in through the API (the UI login is a SPA redirect); the session cookie is shared with the page context.
const resp = await ctx.request.post(`${GRAFANA}/login`, {
  data: { user: "admin", password: process.env.GRAFANA_ADMIN_PASSWORD || "admin" } });
if (!resp.ok()) throw new Error(`Grafana login failed: HTTP ${resp.status()}`);

for (const s of shots.filter((x) => !only || only.includes(x.name))) {
  const page = await ctx.newPage();
  await page.setViewportSize({ width: s.w, height: s.clip ? 1500 : s.h });
  try {
    await page.goto(s.grafana ? GRAFANA + s.url : s.url, { waitUntil: "networkidle", timeout: 45000 });
  } catch (e) { console.log(`  (networkidle timeout for ${s.name}; continuing)`); }
  await page.waitForTimeout(s.settle);
  if (s.click) { await page.getByText(s.click, { exact: false }).first().click(); await page.waitForTimeout(800); }
  const file = join(OUT, `${s.name}.png`);
  await page.screenshot({ path: file, ...(s.clip ? { clip: { x: 0, y: 0, width: s.w, height: s.h } } : {}) });
  console.log("saved", file);
  await page.close();
}
await browser.close();
