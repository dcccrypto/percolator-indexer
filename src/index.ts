import "dotenv/config";
import { Hono } from "hono";
import { serve } from "@hono/node-server";
import { config, createLogger, initSentry, captureException, getSupabase, getConnection, sendCriticalAlert, sendInfoAlert } from "@percolator/shared";
import { MarketDiscovery } from "./services/MarketDiscovery.js";
import { StatsCollector } from "./services/StatsCollector.js";
import { TradeIndexerPolling } from "./services/TradeIndexer.js";
import { InsuranceLPService } from "./services/InsuranceLPService.js";
import { HeliusWebhookManager } from "./services/HeliusWebhookManager.js";
import { webhookRoutes } from "./routes/webhook.js";

// Initialize Sentry first
initSentry("indexer");

const logger = createLogger("indexer");

logger.info("Indexer service starting");

const discovery = new MarketDiscovery();
const statsCollector = new StatsCollector(discovery);
const tradeIndexer = new TradeIndexerPolling();
const insuranceService = new InsuranceLPService(discovery);
const webhookManager = new HeliusWebhookManager();

const app = new Hono();

// Health endpoint with connectivity checks
app.get("/health", async (c) => {
  const checks: { db: boolean; rpc: boolean } = { db: false, rpc: false };
  let status: "ok" | "degraded" | "down" = "ok";
  
  // Check RPC connectivity
  try {
    await getConnection().getSlot();
    checks.rpc = true;
  } catch (err) {
    logger.error("RPC check failed", { error: err instanceof Error ? err.message : err });
    checks.rpc = false;
  }
  
  // Check Supabase connectivity
  try {
    await getSupabase().from("markets").select("id", { count: "exact", head: true });
    checks.db = true;
  } catch (err) {
    logger.error("DB check failed", { error: err instanceof Error ? err.message : err });
    checks.db = false;
  }
  
  // Determine overall status
  const failedChecks = Object.values(checks).filter(v => !v).length;
  if (failedChecks === 0) {
    status = "ok";
  } else if (failedChecks === Object.keys(checks).length) {
    status = "down";
  } else {
    status = "degraded";
  }
  
  const statusCode = status === "down" ? 503 : 200;
  
  return c.json({ status, checks, service: "indexer" }, statusCode);
});

app.route("/", webhookRoutes());

const port = Number(process.env.INDEXER_PORT ?? 3002);

// DB connection monitoring
let dbConnectionLost = false;
setInterval(async () => {
  try {
    await getSupabase().from("markets").select("id", { count: "exact", head: true });
    if (dbConnectionLost) {
      dbConnectionLost = false;
      await sendInfoAlert("Indexer database connection restored");
    }
  } catch (err) {
    if (!dbConnectionLost) {
      dbConnectionLost = true;
      await sendCriticalAlert("Indexer database connection lost", [
        { name: "Error", value: (err instanceof Error ? err.message : String(err)).slice(0, 200), inline: false },
      ]);
    }
    logger.error("DB connection check failed", { error: err });
  }
}, 30_000); // Check every 30s

async function start() {
  await discovery.discover();
  discovery.start();
  statsCollector.start();
  tradeIndexer.start();
  insuranceService.start();
  await webhookManager.start();
  
  serve({ fetch: app.fetch, port }, (info) => {
    logger.info("Indexer service started", { port: info.port });
  });
  
  // Send startup alert
  await sendInfoAlert("Indexer service started", [
    { name: "Port", value: port.toString(), inline: true },
  ]);
}

start().catch((err) => {
  logger.error("Failed to start indexer", { error: err });
  captureException(err, { tags: { context: "indexer-startup" } });
  process.exit(1);
});

async function shutdown(signal: string): Promise<void> {
  logger.info("Shutdown initiated", { signal });
  
  try {
    // Send shutdown alert
    await sendInfoAlert("Indexer service shutting down", [
      { name: "Signal", value: signal, inline: true },
    ]);
    
    // Stop all services (clears timers and intervals)
    logger.info("Stopping market discovery");
    discovery.stop();
    
    logger.info("Stopping stats collector");
    statsCollector.stop();
    
    logger.info("Stopping trade indexer");
    tradeIndexer.stop();
    
    logger.info("Stopping insurance LP service");
    insuranceService.stop();
    
    logger.info("Stopping webhook manager");
    webhookManager.stop();
    
    // Note: Solana connection and Supabase client don't need explicit cleanup
    
    logger.info("Shutdown complete");
    process.exit(0);
  } catch (err) {
    logger.error("Error during shutdown", { error: err });
    captureException(err, { tags: { context: "indexer-shutdown" } });
    process.exit(1);
  }
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
