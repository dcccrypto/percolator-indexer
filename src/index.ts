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

// SEC: Security headers middleware — applied to all responses.
// Hardens the HTTP surface against common attack vectors.
app.use("*", async (c, next) => {
  await next();

  // Prevent MIME-type sniffing (reduces XSS risk from misinterpreted content).
  c.header("X-Content-Type-Options", "nosniff");

  // Prevent clickjacking — this is an API, never meant to be framed.
  c.header("X-Frame-Options", "DENY");

  // Disable browser-side caching of responses (trade data, health status).
  c.header("Cache-Control", "no-store");

  // Remove the default Server header to reduce fingerprinting surface.
  c.header("Server", "");
});

// SEC: Reject non-POST methods on webhook path early (defense in depth).
app.all("/webhook/trades", async (c, next) => {
  if (c.req.method !== "POST") {
    return c.json({ error: "Method not allowed" }, 405);
  }
  return next();
});

// Health endpoint with connectivity checks
// HTTP write path for trades: only POST /webhook/trades (mounted below), after signature verification
// in routes/webhook.ts. Backup indexing uses TradeIndexerPolling (RPC), not this server.
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

// SEC: Validate and sanitize port number at startup to prevent binding to
// unexpected ports or crashing on NaN.
const rawPort = Number(process.env.INDEXER_PORT ?? 3002);
const port = Number.isInteger(rawPort) && rawPort >= 1 && rawPort <= 65535 ? rawPort : 3002;
if (port !== rawPort) {
  logger.warn("Invalid INDEXER_PORT, falling back to default", { raw: process.env.INDEXER_PORT, fallback: port });
}

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
  // Validate NODE_ENV at startup
  const validNodeEnvs = ["production", "development", "test"];
  if (process.env.NODE_ENV && !validNodeEnvs.includes(process.env.NODE_ENV)) {
    logger.error("Invalid NODE_ENV configuration", {
      nodeEnv: process.env.NODE_ENV,
      validOptions: validNodeEnvs.join(", ")
    });
    throw new Error(`Invalid NODE_ENV: ${process.env.NODE_ENV}. Must be one of: ${validNodeEnvs.join(", ")}`);
  }

  // SEC: Validate WEBHOOK_URL to prevent webhook hijacking via misconfigured env var.
  if (config.webhookUrl) {
    try {
      const parsed = new URL(config.webhookUrl);
      if (parsed.protocol !== "https:") {
        logger.warn("WEBHOOK_URL uses non-HTTPS protocol — trades will be sent over insecure connection", {
          protocol: parsed.protocol,
        });
      }
    } catch {
      logger.error("WEBHOOK_URL is not a valid URL — webhook registration will fail", {
        webhookUrl: config.webhookUrl?.slice(0, 50),
      });
    }
  }

  // SEC: Validate SOLANA_RPC_URL to ensure it's a valid HTTPS endpoint.
  if (config.rpcUrl) {
    try {
      const parsed = new URL(config.rpcUrl);
      if (parsed.protocol !== "https:") {
        logger.warn("SOLANA_RPC_URL uses non-HTTPS protocol — RPC calls will be unencrypted");
      }
    } catch {
      logger.error("SOLANA_RPC_URL is not a valid URL — RPC calls will fail");
    }
  }

  // PERC-8235: Verify Supabase connectivity at startup â surface DB issues early
  // instead of letting every sync operation fail silently with "Market sync failed".
  try {
    const { data, error } = await getSupabase().from("markets").select("slab_address").limit(1);
    if (error) {
      logger.error("Supabase connection test FAILED â DB operations will fail", {
        error: error.message,
        code: error.code,
        hint: error.hint,
        details: error.details,
      });
    } else {
      logger.info("Supabase connection test passed", { rowCount: data?.length ?? 0 });
    }
  } catch (dbErr) {
    logger.error("Supabase connection test threw â check DATABASE_URL / SUPABASE_URL", {
      error: dbErr instanceof Error ? dbErr.message : String(dbErr),
    });
  }

  await discovery.start();
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
  logger.error("Failed to start indexer â staying alive for healthcheck + retry", {
    error: err instanceof Error ? err.message : String(err),
  });
  captureException(err, { tags: { context: "indexer-startup" } });
  // Don't exit â keep process alive so Railway healthcheck passes
  // Discovery will retry on its interval and pick up markets when they exist
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
// Trigger rebuild 20260401053418
// cache bust 1775186759
