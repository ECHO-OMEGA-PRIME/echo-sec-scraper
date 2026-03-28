/**
 * echo-sec-scraper — SEC EDGAR Filing Scraper for Financial Intelligence
 *
 * Monitors SEC EDGAR for new filings from tracked companies, parses metadata,
 * extracts key information, uses Workers AI to summarize, stores in D1.
 *
 * SEC EDGAR API: Free, no auth, requires User-Agent header.
 * Rate limit: max 10 requests/second to SEC endpoints.
 */

import { Hono } from "hono";
import { cors } from "hono/cors";

// ---------------------------------------------------------------------------
// Structured logging
// ---------------------------------------------------------------------------

type LogLevel = "info" | "warn" | "error" | "debug";

function log(level: LogLevel, message: string, extra?: Record<string, unknown>): void {
  const entry = { level, message, ts: new Date().toISOString(), worker: "echo-sec-scraper", ...extra };
  if (level === "error") console.error(JSON.stringify(entry));
  else console.log(JSON.stringify(entry));
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface Env {
  DB: D1Database;
  CACHE: KVNamespace;
  AI: Ai;
  BRAIN: Fetcher;
  KNOWLEDGE: Fetcher;
  SWARM: Fetcher;
  ECHO_API_KEY: string;
  USER_AGENT: string;
  MAX_SEC_RPS: string;
  ENVIRONMENT: string;
}

function authOk(c: any): boolean {
  const key = c.req.header('X-Echo-API-Key') || c.req.header('Authorization')?.replace('Bearer ', '');
  return key === c.env.ECHO_API_KEY;
}

interface TrackedCompany {
  id: number;
  ticker: string;
  cik: string;
  name: string;
  filing_types: string;
  enabled: number;
  last_scan: string | null;
  filing_count: number;
  created_at: string;
}

interface Filing {
  id: number;
  company_id: number;
  accession_number: string;
  filing_type: string;
  filed_date: string;
  accepted_date: string;
  primary_document: string;
  filing_url: string;
  description: string;
  size: number;
  ai_summary: string | null;
  key_findings: string | null;
  created_at: string;
}

interface InsiderTrade {
  id: number;
  filing_id: number;
  ticker: string;
  insider_name: string;
  insider_title: string;
  transaction_type: string;
  shares: number;
  price_per_share: number;
  total_value: number;
  ownership_after: number;
  filed_date: string;
  created_at: string;
}

interface EdgarSubmission {
  cik: string;
  entityType: string;
  name: string;
  tickers: string[];
  exchanges: string[];
  filings: {
    recent: {
      accessionNumber: string[];
      filingDate: string[];
      reportDate: string[];
      acceptanceDateTime: string[];
      act: string[];
      form: string[];
      fileNumber: string[];
      filmNumber: string[];
      items: string[];
      size: number[];
      isXBRL: number[];
      isInlineXBRL: number[];
      primaryDocument: string[];
      primaryDocDescription: string[];
    };
  };
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SEC_BASE = "https://data.sec.gov";
const SEC_ARCHIVES = "https://www.sec.gov/Archives/edgar/data";
const SEC_EFTS = "https://efts.sec.gov/LATEST/search-index";

const DEFAULT_COMPANIES: Array<{ ticker: string; cik: string; name: string }> = [
  { ticker: "AAPL", cik: "0000320193", name: "Apple Inc." },
  { ticker: "MSFT", cik: "0000789019", name: "Microsoft Corporation" },
  { ticker: "GOOGL", cik: "0001652044", name: "Alphabet Inc." },
  { ticker: "AMZN", cik: "0001018724", name: "Amazon.com Inc." },
  { ticker: "NVDA", cik: "0001045810", name: "NVIDIA Corporation" },
  { ticker: "TSLA", cik: "0001318605", name: "Tesla Inc." },
  { ticker: "META", cik: "0001326801", name: "Meta Platforms Inc." },
  { ticker: "XOM", cik: "0000034088", name: "Exxon Mobil Corporation" },
  { ticker: "CVX", cik: "0000093410", name: "Chevron Corporation" },
  { ticker: "OXY", cik: "0000797468", name: "Occidental Petroleum Corporation" },
];

const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS tracked_companies (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ticker TEXT UNIQUE NOT NULL,
  cik TEXT NOT NULL,
  name TEXT NOT NULL DEFAULT '',
  filing_types TEXT NOT NULL DEFAULT '["10-K","10-Q","8-K","4"]',
  enabled INTEGER NOT NULL DEFAULT 1,
  last_scan TEXT,
  filing_count INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS filings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  company_id INTEGER NOT NULL,
  accession_number TEXT UNIQUE NOT NULL,
  filing_type TEXT NOT NULL,
  filed_date TEXT NOT NULL,
  accepted_date TEXT NOT NULL DEFAULT '',
  primary_document TEXT NOT NULL DEFAULT '',
  filing_url TEXT NOT NULL DEFAULT '',
  description TEXT NOT NULL DEFAULT '',
  size INTEGER NOT NULL DEFAULT 0,
  ai_summary TEXT,
  key_findings TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (company_id) REFERENCES tracked_companies(id)
);

CREATE INDEX IF NOT EXISTS idx_filings_company ON filings(company_id);
CREATE INDEX IF NOT EXISTS idx_filings_type ON filings(filing_type);
CREATE INDEX IF NOT EXISTS idx_filings_date ON filings(filed_date);
CREATE INDEX IF NOT EXISTS idx_filings_accession ON filings(accession_number);

CREATE TABLE IF NOT EXISTS insider_trades (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  filing_id INTEGER NOT NULL,
  ticker TEXT NOT NULL,
  insider_name TEXT NOT NULL DEFAULT '',
  insider_title TEXT NOT NULL DEFAULT '',
  transaction_type TEXT NOT NULL DEFAULT '',
  shares REAL NOT NULL DEFAULT 0,
  price_per_share REAL NOT NULL DEFAULT 0,
  total_value REAL NOT NULL DEFAULT 0,
  ownership_after REAL NOT NULL DEFAULT 0,
  filed_date TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (filing_id) REFERENCES filings(id)
);

CREATE INDEX IF NOT EXISTS idx_insider_ticker ON insider_trades(ticker);
CREATE INDEX IF NOT EXISTS idx_insider_date ON insider_trades(filed_date);

CREATE TABLE IF NOT EXISTS filing_digests (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  period TEXT NOT NULL,
  content TEXT NOT NULL DEFAULT '',
  filing_count INTEGER NOT NULL DEFAULT 0,
  notable_findings TEXT NOT NULL DEFAULT '[]',
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_digest_period ON filing_digests(period);
`;

// ---------------------------------------------------------------------------
// Rate limiter — max N requests per second to SEC
// ---------------------------------------------------------------------------

class RateLimiter {
  private timestamps: number[] = [];
  private maxPerSecond: number;

  constructor(maxPerSecond: number) {
    this.maxPerSecond = maxPerSecond;
  }

  async throttle(): Promise<void> {
    const now = Date.now();
    this.timestamps = this.timestamps.filter((t) => now - t < 1000);
    if (this.timestamps.length >= this.maxPerSecond) {
      const oldest = this.timestamps[0];
      const waitMs = 1000 - (now - oldest) + 10;
      await new Promise((r) => setTimeout(r, waitMs));
    }
    this.timestamps.push(Date.now());
  }
}

// ---------------------------------------------------------------------------
// SEC EDGAR API Client
// ---------------------------------------------------------------------------

class EdgarClient {
  private userAgent: string;
  private limiter: RateLimiter;

  constructor(userAgent: string, maxRps: number) {
    this.userAgent = userAgent;
    this.limiter = new RateLimiter(maxRps);
  }

  private async secFetch(url: string): Promise<Response> {
    await this.limiter.throttle();
    const res = await fetch(url, {
      headers: {
        "User-Agent": this.userAgent,
        Accept: "application/json",
      },
    });
    if (!res.ok) {
      throw new Error(`SEC fetch ${url} returned ${res.status}: ${await res.text().catch(() => "")}`);
    }
    return res;
  }

  async getCompanySubmissions(cik: string): Promise<EdgarSubmission> {
    const paddedCik = cik.replace(/^0+/, "").padStart(10, "0");
    const url = `${SEC_BASE}/submissions/CIK${paddedCik}.json`;
    const res = await this.secFetch(url);
    return res.json() as Promise<EdgarSubmission>;
  }

  async getFilingDocument(cik: string, accession: string, document: string): Promise<string> {
    const cleanCik = cik.replace(/^0+/, "");
    const cleanAccession = accession.replace(/-/g, "");
    const url = `${SEC_ARCHIVES}/${cleanCik}/${cleanAccession}/${document}`;
    const res = await this.secFetch(url);
    return res.text();
  }

  buildFilingUrl(cik: string, accession: string, document: string): string {
    const cleanCik = cik.replace(/^0+/, "");
    const cleanAccession = accession.replace(/-/g, "");
    return `${SEC_ARCHIVES}/${cleanCik}/${cleanAccession}/${document}`;
  }
}

// ---------------------------------------------------------------------------
// DB helpers
// ---------------------------------------------------------------------------

async function initDb(db: D1Database): Promise<void> {
  const statements = SCHEMA_SQL.split(";")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
  for (const sql of statements) {
    await db.prepare(sql).run();
  }
}

async function seedDefaults(db: D1Database): Promise<number> {
  let added = 0;
  for (const c of DEFAULT_COMPANIES) {
    const existing = await db
      .prepare("SELECT id FROM tracked_companies WHERE ticker = ?")
      .bind(c.ticker)
      .first();
    if (!existing) {
      await db
        .prepare(
          "INSERT INTO tracked_companies (ticker, cik, name) VALUES (?, ?, ?)"
        )
        .bind(c.ticker, c.cik, c.name)
        .run();
      added++;
    }
  }
  return added;
}

// ---------------------------------------------------------------------------
// Filing scanner
// ---------------------------------------------------------------------------

async function scanCompany(
  db: D1Database,
  edgar: EdgarClient,
  company: TrackedCompany,
  ai: Ai,
  cache: KVNamespace
): Promise<{ newFilings: number; errors: string[] }> {
  const errors: string[] = [];
  let newFilings = 0;
  const wantedTypes: string[] = JSON.parse(company.filing_types);

  try {
    const submissions = await edgar.getCompanySubmissions(company.cik);
    const recent = submissions.filings.recent;
    const count = recent.accessionNumber.length;

    // Update company name from SEC if we have a generic one
    if (submissions.name && submissions.name !== company.name) {
      await db
        .prepare("UPDATE tracked_companies SET name = ? WHERE id = ?")
        .bind(submissions.name, company.id)
        .run();
    }

    for (let i = 0; i < Math.min(count, 50); i++) {
      const form = recent.form[i];
      if (!wantedTypes.includes(form)) continue;

      const accession = recent.accessionNumber[i];
      const existing = await db
        .prepare("SELECT id FROM filings WHERE accession_number = ?")
        .bind(accession)
        .first();
      if (existing) continue;

      const filedDate = recent.filingDate[i];
      const acceptedDate = recent.acceptanceDateTime[i] || filedDate;
      const primaryDoc = recent.primaryDocument[i] || "";
      const description = recent.primaryDocDescription[i] || "";
      const size = recent.size[i] || 0;
      const filingUrl = edgar.buildFilingUrl(company.cik, accession, primaryDoc);

      await db
        .prepare(
          `INSERT INTO filings
           (company_id, accession_number, filing_type, filed_date, accepted_date,
            primary_document, filing_url, description, size)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
        )
        .bind(
          company.id,
          accession,
          form,
          filedDate,
          acceptedDate,
          primaryDoc,
          filingUrl,
          description,
          size
        )
        .run();

      newFilings++;

      // For Form 4 (insider trading), attempt to parse
      if (form === "4" && primaryDoc) {
        try {
          await parseInsiderFiling(db, edgar, company, accession, primaryDoc, filedDate);
        } catch (e: any) {
          errors.push(`Insider parse ${accession}: ${e.message}`);
        }
      }

      // AI summarize large filings (10-K, 10-Q, 8-K) — background, don't block
      if (["10-K", "10-Q", "8-K"].includes(form)) {
        try {
          await generateFilingSummary(db, edgar, ai, cache, company, accession, primaryDoc, form);
        } catch (e: any) {
          errors.push(`AI summary ${accession}: ${e.message}`);
        }
      }
    }

    // Update last_scan and filing_count
    const totalFilings = await db
      .prepare("SELECT COUNT(*) as cnt FROM filings WHERE company_id = ?")
      .bind(company.id)
      .first<{ cnt: number }>();

    await db
      .prepare(
        "UPDATE tracked_companies SET last_scan = datetime('now'), filing_count = ? WHERE id = ?"
      )
      .bind(totalFilings?.cnt ?? 0, company.id)
      .run();
  } catch (e: any) {
    errors.push(`Scan ${company.ticker}: ${e.message}`);
  }

  return { newFilings, errors };
}

async function parseInsiderFiling(
  db: D1Database,
  edgar: EdgarClient,
  company: TrackedCompany,
  accession: string,
  primaryDoc: string,
  filedDate: string
): Promise<void> {
  // Get the filing record
  const filing = await db
    .prepare("SELECT id FROM filings WHERE accession_number = ?")
    .bind(accession)
    .first<{ id: number }>();
  if (!filing) return;

  // Fetch the XML document
  let docText: string;
  try {
    docText = await edgar.getFilingDocument(company.cik, accession, primaryDoc);
  } catch (e) {
    log("warn", "Failed to fetch filing document for insider trade parsing", { error: (e as Error)?.message || String(e), cik: company.cik, accession });
    return;
  }

  // Parse insider trading data from the XML
  const insiderName = extractXmlValue(docText, "rptOwnerName") || "Unknown";
  const insiderTitle = extractXmlValue(docText, "officerTitle") || extractXmlValue(docText, "rptOwnerRelationship") || "";

  // Extract all transactions from the document
  const transactions = extractTransactions(docText);

  for (const tx of transactions) {
    await db
      .prepare(
        `INSERT INTO insider_trades
         (filing_id, ticker, insider_name, insider_title, transaction_type,
          shares, price_per_share, total_value, ownership_after, filed_date)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      )
      .bind(
        filing.id,
        company.ticker,
        insiderName,
        insiderTitle,
        tx.type,
        tx.shares,
        tx.price,
        tx.shares * tx.price,
        tx.ownershipAfter,
        filedDate
      )
      .run();
  }
}

function extractXmlValue(xml: string, tag: string): string | null {
  const regex = new RegExp(`<${tag}>([^<]*)</${tag}>`, "i");
  const match = xml.match(regex);
  return match ? match[1].trim() : null;
}

function extractTransactions(xml: string): Array<{
  type: string;
  shares: number;
  price: number;
  ownershipAfter: number;
}> {
  const transactions: Array<{
    type: string;
    shares: number;
    price: number;
    ownershipAfter: number;
  }> = [];

  // Match non-derivative transactions
  const txBlocks = xml.match(/<nonDerivativeTransaction>[\s\S]*?<\/nonDerivativeTransaction>/gi) || [];

  for (const block of txBlocks) {
    const code = extractXmlValue(block, "transactionCode") || "P";
    const sharesStr = extractXmlValue(block, "transactionShares") ||
      extractXmlValue(block, "value") || "0";
    const priceStr = extractXmlValue(block, "transactionPricePerShare") ||
      extractXmlValue(block, "value") || "0";
    const ownershipStr = extractXmlValue(block, "sharesOwnedFollowingTransaction") ||
      extractXmlValue(block, "value") || "0";

    const typeMap: Record<string, string> = {
      P: "Purchase",
      S: "Sale",
      A: "Award",
      D: "Disposition",
      F: "Tax Payment",
      M: "Exercise",
      G: "Gift",
      C: "Conversion",
      J: "Other",
    };

    transactions.push({
      type: typeMap[code] || code,
      shares: parseFloat(sharesStr) || 0,
      price: parseFloat(priceStr) || 0,
      ownershipAfter: parseFloat(ownershipStr) || 0,
    });
  }

  // If no structured transactions found, try a simpler parse
  if (transactions.length === 0) {
    const shares = parseFloat(extractXmlValue(xml, "transactionShares") || extractXmlValue(xml, "value") || "0") || 0;
    const price = parseFloat(extractXmlValue(xml, "transactionPricePerShare") || "0") || 0;
    const code = extractXmlValue(xml, "transactionCode") || "P";
    const ownershipStr = extractXmlValue(xml, "sharesOwnedFollowingTransaction") || "0";

    if (shares > 0) {
      const typeMap: Record<string, string> = {
        P: "Purchase", S: "Sale", A: "Award", D: "Disposition",
        F: "Tax Payment", M: "Exercise", G: "Gift",
      };
      transactions.push({
        type: typeMap[code] || code,
        shares,
        price,
        ownershipAfter: parseFloat(ownershipStr) || 0,
      });
    }
  }

  return transactions;
}

// ---------------------------------------------------------------------------
// AI Summary
// ---------------------------------------------------------------------------

async function generateFilingSummary(
  db: D1Database,
  edgar: EdgarClient,
  ai: Ai,
  cache: KVNamespace,
  company: TrackedCompany,
  accession: string,
  primaryDoc: string,
  filingType: string
): Promise<void> {
  const cacheKey = `summary:${accession}`;
  const cached = await cache.get(cacheKey);
  if (cached) {
    await db
      .prepare("UPDATE filings SET ai_summary = ? WHERE accession_number = ?")
      .bind(cached, accession)
      .run();
    return;
  }

  // Fetch first portion of the filing document for summarization
  let docText: string;
  try {
    docText = await edgar.getFilingDocument(company.cik, accession, primaryDoc);
  } catch (e) {
    log("warn", "Failed to fetch filing document for AI summarization", { error: (e as Error)?.message || String(e), cik: company.cik, accession });
    return;
  }

  // Truncate to ~6000 chars to fit within model context
  const truncated = docText.substring(0, 6000).replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();

  if (truncated.length < 100) return; // Too short, likely not useful

  const prompt = `You are a financial analyst. Summarize this SEC ${filingType} filing from ${company.name} (${company.ticker}).
Provide:
1. A 2-3 sentence executive summary
2. Key financial metrics mentioned (revenue, earnings, margins)
3. Notable risks or events disclosed
4. Any forward-looking guidance

Filing content:
${truncated}

Respond in JSON format:
{"summary":"...","key_metrics":["..."],"risks":["..."],"guidance":"..."}`;

  try {
    const result = await ai.run("@cf/meta/llama-3.1-8b-instruct" as any, {
      messages: [{ role: "user", content: prompt }],
      max_tokens: 1024,
    }) as any;

    const responseText = result?.response || result?.result?.response || "";
    if (!responseText) return;

    // Store the summary
    await db
      .prepare("UPDATE filings SET ai_summary = ? WHERE accession_number = ?")
      .bind(responseText, accession)
      .run();

    // Extract key findings
    try {
      const parsed = JSON.parse(responseText);
      const findings = [
        ...(parsed.key_metrics || []),
        ...(parsed.risks || []),
        parsed.guidance ? `Guidance: ${parsed.guidance}` : "",
      ].filter(Boolean);

      if (findings.length > 0) {
        await db
          .prepare("UPDATE filings SET key_findings = ? WHERE accession_number = ?")
          .bind(JSON.stringify(findings), accession)
          .run();
      }
    } catch (e) {
      log("warn", "AI response was not valid JSON, storing raw key_findings", { error: (e as Error)?.message || String(e), accession });
      await db
        .prepare("UPDATE filings SET key_findings = ? WHERE accession_number = ?")
        .bind(JSON.stringify([responseText.substring(0, 500)]), accession)
        .run();
    }

    // Cache for 24 hours
    await cache.put(cacheKey, responseText, { expirationTtl: 86400 });
  } catch (e: any) {
    // AI failure is non-fatal — filing is still stored
    console.error(`AI summary failed for ${accession}: ${e.message}`);
  }
}

// ---------------------------------------------------------------------------
// Digest generation
// ---------------------------------------------------------------------------

async function generateDigest(db: D1Database, ai: Ai): Promise<string> {
  const today = new Date().toISOString().split("T")[0];

  // Get all filings from last 24 hours
  const recentFilings = await db
    .prepare(
      `SELECT f.*, tc.ticker, tc.name as company_name
       FROM filings f
       JOIN tracked_companies tc ON f.company_id = tc.id
       WHERE f.filed_date >= date('now', '-1 day')
       ORDER BY f.filed_date DESC
       LIMIT 50`
    )
    .all<Filing & { ticker: string; company_name: string }>();

  const filings = recentFilings.results || [];

  if (filings.length === 0) {
    const digest = `SEC Filing Digest for ${today}\n\nNo new filings detected in the past 24 hours for tracked companies.`;
    await db
      .prepare(
        "INSERT INTO filing_digests (period, content, filing_count, notable_findings) VALUES (?, ?, ?, ?)"
      )
      .bind(today, digest, 0, "[]")
      .run();
    return digest;
  }

  // Build digest content
  const byType: Record<string, number> = {};
  const summaryLines: string[] = [];
  const notableFindings: string[] = [];

  for (const f of filings) {
    byType[f.filing_type] = (byType[f.filing_type] || 0) + 1;
    const ticker = (f as any).ticker || "???";
    const companyName = (f as any).company_name || "";
    summaryLines.push(
      `- ${ticker} (${companyName}): ${f.filing_type} filed ${f.filed_date}${f.description ? " — " + f.description : ""}`
    );

    if (f.key_findings) {
      try {
        const findings = JSON.parse(f.key_findings);
        for (const finding of findings) {
          if (finding && typeof finding === "string" && finding.length > 10) {
            notableFindings.push(`[${ticker}] ${finding}`);
          }
        }
      } catch (e) { log("warn", "Failed to parse key_findings JSON in digest", { error: (e as Error)?.message || String(e), ticker }); }
    }
  }

  const typeBreakdown = Object.entries(byType)
    .map(([t, c]) => `${t}: ${c}`)
    .join(", ");

  let digest = `SEC Filing Digest for ${today}\n`;
  digest += `${"=".repeat(40)}\n\n`;
  digest += `Total filings: ${filings.length}\n`;
  digest += `By type: ${typeBreakdown}\n\n`;
  digest += `Filings:\n${summaryLines.join("\n")}\n`;

  if (notableFindings.length > 0) {
    digest += `\nNotable Findings:\n`;
    for (const nf of notableFindings.slice(0, 15)) {
      digest += `  ${nf}\n`;
    }
  }

  // Use AI to create executive summary if we have enough data
  if (filings.length >= 3) {
    try {
      const aiResult = await ai.run("@cf/meta/llama-3.1-8b-instruct" as any, {
        messages: [
          {
            role: "user",
            content: `Summarize these SEC filings into a 3-sentence executive briefing for an investor:\n${summaryLines.join("\n")}\n${notableFindings.length > 0 ? "\nKey findings: " + notableFindings.slice(0, 5).join("; ") : ""}`,
          },
        ],
        max_tokens: 256,
      }) as any;
      const execSummary = aiResult?.response || "";
      if (execSummary) {
        digest = `EXECUTIVE SUMMARY:\n${execSummary}\n\n${digest}`;
      }
    } catch (e) { log("warn", "AI executive summary generation failed for digest", { error: (e as Error)?.message || String(e) }); }
  }

  await db
    .prepare(
      "INSERT INTO filing_digests (period, content, filing_count, notable_findings) VALUES (?, ?, ?, ?)"
    )
    .bind(today, digest, filings.length, JSON.stringify(notableFindings.slice(0, 20)))
    .run();

  return digest;
}

// ---------------------------------------------------------------------------
// Hono App
// ---------------------------------------------------------------------------

const app = new Hono<{ Bindings: Env }>();

app.use("*", cors({ origin: "*" }));
// Security headers middleware
app.use('*', async (c, next) => {
  await next();
  c.res.headers.set('X-Content-Type-Options', 'nosniff');
  c.res.headers.set('X-Frame-Options', 'DENY');
  c.res.headers.set('X-XSS-Protection', '1; mode=block');
  c.res.headers.set('Referrer-Policy', 'strict-origin-when-cross-origin');
  c.res.headers.set('Strict-Transport-Security', 'max-age=31536000; includeSubDomains');
});


// ---- Health & Stats -------------------------------------------------------

app.get("/health", async (c) => {
  const env = c.env;
  let dbOk = false;
  let companyCount = 0;
  let filingCount = 0;

  try {
    await initDb(env.DB);
    const companies = await env.DB.prepare("SELECT COUNT(*) as cnt FROM tracked_companies").first<{ cnt: number }>();
    const filings = await env.DB.prepare("SELECT COUNT(*) as cnt FROM filings").first<{ cnt: number }>();
    companyCount = companies?.cnt ?? 0;
    filingCount = filings?.cnt ?? 0;
    dbOk = true;
  } catch (e) { log("warn", "Health check DB query failed", { error: (e as Error)?.message || String(e) }); }

  return c.json({
    status: dbOk ? "healthy" : "degraded",
    service: "echo-sec-scraper",
    version: "1.0.0",
    timestamp: new Date().toISOString(),
    db: dbOk ? "connected" : "error",
    tracked_companies: companyCount,
    total_filings: filingCount,
    sec_user_agent: env.USER_AGENT,
    crons: ["every 6 hours (scan)", "daily 6am UTC (digest)"],
  });
});

app.get("/stats", async (c) => {
  const db = c.env.DB;
  await initDb(db);

  const companies = await db.prepare("SELECT COUNT(*) as cnt FROM tracked_companies WHERE enabled = 1").first<{ cnt: number }>();
  const totalFilings = await db.prepare("SELECT COUNT(*) as cnt FROM filings").first<{ cnt: number }>();
  const todayFilings = await db
    .prepare("SELECT COUNT(*) as cnt FROM filings WHERE filed_date = date('now')")
    .first<{ cnt: number }>();
  const typeBreakdown = await db
    .prepare(
      "SELECT filing_type, COUNT(*) as cnt FROM filings GROUP BY filing_type ORDER BY cnt DESC"
    )
    .all<{ filing_type: string; cnt: number }>();
  const insiderCount = await db.prepare("SELECT COUNT(*) as cnt FROM insider_trades").first<{ cnt: number }>();
  const latestScan = await db
    .prepare("SELECT MAX(last_scan) as latest FROM tracked_companies")
    .first<{ latest: string | null }>();
  const digestCount = await db.prepare("SELECT COUNT(*) as cnt FROM filing_digests").first<{ cnt: number }>();

  return c.json({
    tracked_companies: companies?.cnt ?? 0,
    total_filings: totalFilings?.cnt ?? 0,
    filings_today: todayFilings?.cnt ?? 0,
    insider_trades: insiderCount?.cnt ?? 0,
    digests: digestCount?.cnt ?? 0,
    latest_scan: latestScan?.latest || "never",
    filing_types: (typeBreakdown.results || []).reduce(
      (acc, r) => ({ ...acc, [r.filing_type]: r.cnt }),
      {} as Record<string, number>
    ),
  });
});

// ---- Company Management ---------------------------------------------------

app.get("/companies", async (c) => {
  const db = c.env.DB;
  await initDb(db);

  const companies = await db
    .prepare("SELECT * FROM tracked_companies ORDER BY ticker ASC")
    .all<TrackedCompany>();

  return c.json({
    count: companies.results?.length ?? 0,
    companies: (companies.results || []).map((co) => ({
      ...co,
      filing_types: JSON.parse(co.filing_types),
    })),
  });
});

app.post("/companies/add", async (c) => {
  if (!authOk(c)) return c.json({ error: 'Unauthorized' }, 401);
  const db = c.env.DB;
  await initDb(db);

  const body = await c.req.json<{
    ticker: string;
    cik?: string;
    name?: string;
    filing_types?: string[];
  }>();

  if (!body.ticker) {
    return c.json({ error: "ticker is required" }, 400);
  }

  const ticker = body.ticker.toUpperCase().trim();

  // Check if already tracked
  const existing = await db
    .prepare("SELECT id FROM tracked_companies WHERE ticker = ?")
    .bind(ticker)
    .first();

  if (existing) {
    return c.json({ error: `${ticker} is already tracked`, id: (existing as any).id }, 409);
  }

  // If no CIK provided, try to look it up via SEC
  let cik = body.cik || "";
  let name = body.name || ticker;

  if (!cik) {
    try {
      const edgar = new EdgarClient(c.env.USER_AGENT, parseInt(c.env.MAX_SEC_RPS) || 10);
      // Try to search for the ticker — SEC tickers endpoint
      const tickerUrl = `https://www.sec.gov/cgi-bin/browse-edgar?company=&CIK=${ticker}&type=&dateb=&owner=include&count=1&search_text=&action=getcompany`;
      await edgar["limiter"].throttle();
      const res = await fetch(tickerUrl, {
        headers: { "User-Agent": c.env.USER_AGENT },
      });
      const html = await res.text();
      // Extract CIK from the page
      const cikMatch = html.match(/CIK=(\d{10})/);
      if (cikMatch) {
        cik = cikMatch[1];
      }
      // Extract company name
      const nameMatch = html.match(/companyName">([^<]+)</);
      if (nameMatch && !body.name) {
        name = nameMatch[1].trim();
      }
    } catch (e) {
      log("warn", "CIK auto-resolve from SEC EDGAR failed", { error: (e as Error)?.message || String(e), ticker });
      if (!cik) {
        return c.json(
          { error: "Could not auto-resolve CIK for ticker. Please provide cik parameter." },
          400
        );
      }
    }
  }

  if (!cik) {
    return c.json({ error: "CIK is required — could not auto-resolve" }, 400);
  }

  const filingTypes = body.filing_types || ["10-K", "10-Q", "8-K", "4"];

  const result = await db
    .prepare(
      "INSERT INTO tracked_companies (ticker, cik, name, filing_types) VALUES (?, ?, ?, ?)"
    )
    .bind(ticker, cik.padStart(10, "0"), name, JSON.stringify(filingTypes))
    .run();

  return c.json({
    success: true,
    id: result.meta?.last_row_id,
    ticker,
    cik: cik.padStart(10, "0"),
    name,
    filing_types: filingTypes,
  });
});

app.delete("/companies/:id", async (c) => {
  if (!authOk(c)) return c.json({ error: 'Unauthorized' }, 401);
  const db = c.env.DB;
  const id = parseInt(c.req.param("id"));

  if (isNaN(id)) {
    return c.json({ error: "Invalid company id" }, 400);
  }

  const company = await db
    .prepare("SELECT ticker FROM tracked_companies WHERE id = ?")
    .bind(id)
    .first<{ ticker: string }>();

  if (!company) {
    return c.json({ error: "Company not found" }, 404);
  }

  // Delete associated data
  await db
    .prepare(
      "DELETE FROM insider_trades WHERE filing_id IN (SELECT id FROM filings WHERE company_id = ?)"
    )
    .bind(id)
    .run();
  await db.prepare("DELETE FROM filings WHERE company_id = ?").bind(id).run();
  await db.prepare("DELETE FROM tracked_companies WHERE id = ?").bind(id).run();

  return c.json({ success: true, deleted: company.ticker });
});

// ---- Filing Scan ----------------------------------------------------------

app.post("/scan", async (c) => {
  if (!authOk(c)) return c.json({ error: 'Unauthorized' }, 401);
  const env = c.env;
  await initDb(env.DB);
  await seedDefaults(env.DB);

  const edgar = new EdgarClient(env.USER_AGENT, parseInt(env.MAX_SEC_RPS) || 10);

  const companies = await env.DB
    .prepare("SELECT * FROM tracked_companies WHERE enabled = 1")
    .all<TrackedCompany>();

  const results: Array<{ ticker: string; newFilings: number; errors: string[] }> = [];
  let totalNew = 0;

  for (const company of companies.results || []) {
    const { newFilings, errors } = await scanCompany(env.DB, edgar, company, env.AI, env.CACHE);
    results.push({ ticker: company.ticker, newFilings, errors });
    totalNew += newFilings;
  }

  // Report to Shared Brain if we found new filings
  if (totalNew > 0) {
    try {
      await env.BRAIN.fetch("https://brain/ingest", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          instance_id: "echo-sec-scraper",
          role: "assistant",
          content: `SEC SCAN: Found ${totalNew} new filings across ${results.filter((r) => r.newFilings > 0).length} companies. ${results
            .filter((r) => r.newFilings > 0)
            .map((r) => `${r.ticker}: ${r.newFilings}`)
            .join(", ")}`,
          importance: 6,
          tags: ["sec", "filings", "scan"],
        }),
      });
    } catch (e) { log("warn", "Shared Brain ingest failed after scan", { error: (e as Error)?.message || String(e) }); }
  }

  return c.json({
    success: true,
    timestamp: new Date().toISOString(),
    companies_scanned: (companies.results || []).length,
    new_filings_total: totalNew,
    results,
  });
});

// ---- Filings Query --------------------------------------------------------

app.get("/filings", async (c) => {
  const db = c.env.DB;
  await initDb(db);

  const ticker = c.req.query("ticker");
  const type = c.req.query("type");
  const startDate = c.req.query("start_date");
  const endDate = c.req.query("end_date");
  const search = c.req.query("q");
  const limit = Math.min(parseInt(c.req.query("limit") || "50"), 200);
  const offset = parseInt(c.req.query("offset") || "0");

  let sql =
    "SELECT f.*, tc.ticker, tc.name as company_name FROM filings f JOIN tracked_companies tc ON f.company_id = tc.id WHERE 1=1";
  const params: any[] = [];

  if (ticker) {
    sql += " AND tc.ticker = ?";
    params.push(ticker.toUpperCase());
  }
  if (type) {
    sql += " AND f.filing_type = ?";
    params.push(type);
  }
  if (startDate) {
    sql += " AND f.filed_date >= ?";
    params.push(startDate);
  }
  if (endDate) {
    sql += " AND f.filed_date <= ?";
    params.push(endDate);
  }
  if (search) {
    sql += " AND (f.description LIKE ? OR f.ai_summary LIKE ? OR f.key_findings LIKE ?)";
    const searchTerm = `%${search}%`;
    params.push(searchTerm, searchTerm, searchTerm);
  }

  // Count total
  const countSql = sql.replace("SELECT f.*, tc.ticker, tc.name as company_name", "SELECT COUNT(*) as cnt");
  const countStmt = db.prepare(countSql);
  const countResult = await (params.length > 0 ? countStmt.bind(...params) : countStmt).first<{ cnt: number }>();

  sql += " ORDER BY f.filed_date DESC, f.id DESC LIMIT ? OFFSET ?";
  params.push(limit, offset);

  const stmt = db.prepare(sql);
  const filings = await (params.length > 0 ? stmt.bind(...params) : stmt).all();

  return c.json({
    total: countResult?.cnt ?? 0,
    limit,
    offset,
    filings: filings.results || [],
  });
});

app.get("/filings/recent", async (c) => {
  const db = c.env.DB;
  await initDb(db);

  const limit = Math.min(parseInt(c.req.query("limit") || "25"), 100);

  const filings = await db
    .prepare(
      `SELECT f.*, tc.ticker, tc.name as company_name
       FROM filings f
       JOIN tracked_companies tc ON f.company_id = tc.id
       ORDER BY f.filed_date DESC, f.id DESC
       LIMIT ?`
    )
    .bind(limit)
    .all();

  return c.json({
    count: filings.results?.length ?? 0,
    filings: filings.results || [],
  });
});

app.get("/filings/:accession", async (c) => {
  const db = c.env.DB;
  await initDb(db);

  const accession = c.req.param("accession");

  const filing = await db
    .prepare(
      `SELECT f.*, tc.ticker, tc.name as company_name, tc.cik
       FROM filings f
       JOIN tracked_companies tc ON f.company_id = tc.id
       WHERE f.accession_number = ?`
    )
    .bind(accession)
    .first();

  if (!filing) {
    return c.json({ error: "Filing not found" }, 404);
  }

  // Get insider trades if Form 4
  let insiderTrades: InsiderTrade[] = [];
  if ((filing as any).filing_type === "4") {
    const trades = await db
      .prepare("SELECT * FROM insider_trades WHERE filing_id = ?")
      .bind((filing as any).id)
      .all<InsiderTrade>();
    insiderTrades = trades.results || [];
  }

  // Parse AI summary and key findings if they're JSON strings
  let parsedSummary = (filing as any).ai_summary;
  let parsedFindings = (filing as any).key_findings;
  try {
    if (parsedSummary) parsedSummary = JSON.parse(parsedSummary);
  } catch (e) { log("warn", "Failed to parse ai_summary JSON, leaving as string", { error: (e as Error)?.message || String(e) }); }
  try {
    if (parsedFindings) parsedFindings = JSON.parse(parsedFindings);
  } catch (e) { log("warn", "Failed to parse key_findings JSON, leaving as string", { error: (e as Error)?.message || String(e) }); }

  return c.json({
    ...filing,
    ai_summary_parsed: parsedSummary,
    key_findings_parsed: parsedFindings,
    insider_trades: insiderTrades,
  });
});

app.post("/filings/:accession/analyze", async (c) => {
  if (!authOk(c)) return c.json({ error: 'Unauthorized' }, 401);
  const env = c.env;
  await initDb(env.DB);

  const accession = c.req.param("accession");

  const filing = await env.DB
    .prepare(
      `SELECT f.*, tc.ticker, tc.name as company_name, tc.cik
       FROM filings f
       JOIN tracked_companies tc ON f.company_id = tc.id
       WHERE f.accession_number = ?`
    )
    .bind(accession)
    .first<Filing & { ticker: string; company_name: string; cik: string }>();

  if (!filing) {
    return c.json({ error: "Filing not found" }, 404);
  }

  const edgar = new EdgarClient(env.USER_AGENT, parseInt(env.MAX_SEC_RPS) || 10);

  const company: TrackedCompany = {
    id: filing.company_id,
    ticker: filing.ticker,
    cik: filing.cik,
    name: filing.company_name,
    filing_types: "[]",
    enabled: 1,
    last_scan: null,
    filing_count: 0,
    created_at: "",
  };

  await generateFilingSummary(
    env.DB,
    edgar,
    env.AI,
    env.CACHE,
    company,
    accession,
    filing.primary_document,
    filing.filing_type
  );

  // Re-fetch to get updated summary
  const updated = await env.DB
    .prepare("SELECT ai_summary, key_findings FROM filings WHERE accession_number = ?")
    .bind(accession)
    .first<{ ai_summary: string | null; key_findings: string | null }>();

  return c.json({
    success: true,
    accession,
    ai_summary: updated?.ai_summary || null,
    key_findings: updated?.key_findings || null,
  });
});

// ---- Insider Trading ------------------------------------------------------

app.get("/insider", async (c) => {
  const db = c.env.DB;
  await initDb(db);

  const ticker = c.req.query("ticker");
  const txType = c.req.query("type");
  const startDate = c.req.query("start_date");
  const limit = Math.min(parseInt(c.req.query("limit") || "50"), 200);
  const offset = parseInt(c.req.query("offset") || "0");

  let sql = "SELECT * FROM insider_trades WHERE 1=1";
  const params: any[] = [];

  if (ticker) {
    sql += " AND ticker = ?";
    params.push(ticker.toUpperCase());
  }
  if (txType) {
    sql += " AND transaction_type = ?";
    params.push(txType);
  }
  if (startDate) {
    sql += " AND filed_date >= ?";
    params.push(startDate);
  }

  sql += " ORDER BY filed_date DESC, id DESC LIMIT ? OFFSET ?";
  params.push(limit, offset);

  const stmt = db.prepare(sql);
  const trades = await (params.length > 0 ? stmt.bind(...params) : stmt).all<InsiderTrade>();

  // Get summary stats
  const statsSql = ticker
    ? "SELECT COUNT(*) as cnt, SUM(total_value) as total_val FROM insider_trades WHERE ticker = ?"
    : "SELECT COUNT(*) as cnt, SUM(total_value) as total_val FROM insider_trades";
  const statsStmt = ticker ? db.prepare(statsSql).bind(ticker.toUpperCase()) : db.prepare(statsSql);
  const stats = await statsStmt.first<{ cnt: number; total_val: number }>();

  return c.json({
    total: stats?.cnt ?? 0,
    total_value: stats?.total_val ?? 0,
    limit,
    offset,
    trades: trades.results || [],
  });
});

// ---- Digest ---------------------------------------------------------------

app.get("/digest", async (c) => {
  const db = c.env.DB;
  await initDb(db);

  const latest = await db
    .prepare("SELECT * FROM filing_digests ORDER BY created_at DESC LIMIT 1")
    .first();

  if (!latest) {
    return c.json({ message: "No digests generated yet. Trigger a scan first or wait for the daily cron." });
  }

  return c.json(latest);
});

app.post("/digest/generate", async (c) => {
  if (!authOk(c)) return c.json({ error: 'Unauthorized' }, 401);
  const env = c.env;
  await initDb(env.DB);

  const digest = await generateDigest(env.DB, env.AI);

  return c.json({ success: true, digest });
});

// ---- Init / Seed ----------------------------------------------------------

app.post("/init", async (c) => {
  if (!authOk(c)) return c.json({ error: 'Unauthorized' }, 401);
  const db = c.env.DB;
  await initDb(db);
  const seeded = await seedDefaults(db);
  return c.json({
    success: true,
    message: `Database initialized. ${seeded} default companies seeded.`,
  });
});

// ---- Catch-all ------------------------------------------------------------

app.get("/", (c) => {
  return c.json({
    service: "echo-sec-scraper",
    version: "1.0.0",
    description: "SEC EDGAR Filing Scraper for Financial Intelligence",
    endpoints: [
      "GET  /health",
      "GET  /stats",
      "POST /init",
      "GET  /companies",
      "POST /companies/add",
      "DELETE /companies/:id",
      "POST /scan",
      "GET  /filings",
      "GET  /filings/recent",
      "GET  /filings/:accession",
      "POST /filings/:accession/analyze",
      "GET  /insider",
      "GET  /digest",
      "POST /digest/generate",
    ],
    sec_info: {
      data_source: "SEC EDGAR (https://www.sec.gov/edgar)",
      rate_limit: "10 requests/second",
      filing_types: ["10-K", "10-Q", "8-K", "Form 4"],
      user_agent: "Required for SEC EDGAR API access",
    },
  });
});

// ---------------------------------------------------------------------------
// Cron handler
// ---------------------------------------------------------------------------

async function handleCron(event: ScheduledEvent, env: Env): Promise<void> {
  await initDb(env.DB);
  await seedDefaults(env.DB);

  const hour = new Date(event.scheduledTime).getUTCHours();

  if (hour === 6) {
    // Daily 6am UTC — generate digest
    console.log("Cron: Generating daily filing digest");
    await generateDigest(env.DB, env.AI);

    // Also do a scan
    const edgar = new EdgarClient(env.USER_AGENT, parseInt(env.MAX_SEC_RPS) || 10);
    const companies = await env.DB
      .prepare("SELECT * FROM tracked_companies WHERE enabled = 1")
      .all<TrackedCompany>();

    let totalNew = 0;
    for (const company of companies.results || []) {
      const { newFilings } = await scanCompany(env.DB, edgar, company, env.AI, env.CACHE);
      totalNew += newFilings;
    }

    // Post digest to MoltBook
    try {
      const latestDigest = await env.DB
        .prepare("SELECT content FROM filing_digests ORDER BY created_at DESC LIMIT 1")
        .first<{ content: string }>();

      await env.SWARM.fetch("https://swarm/moltbook/post", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          author_id: "echo-sec-scraper",
          author_name: "SEC Scraper",
          author_type: "agent",
          content: `SEC Daily Digest: ${totalNew} new filings. ${latestDigest?.content?.substring(0, 300) || "No digest available."}`,
          mood: totalNew > 0 ? "excited" : "neutral",
          tags: ["sec", "digest", "filings"],
        }),
      });
    } catch (e) { log("warn", "MoltBook digest post failed", { error: (e as Error)?.message || String(e) }); }
  } else {
    // Every 6 hours — scan for new filings
    console.log("Cron: Scanning for new SEC filings");
    const edgar = new EdgarClient(env.USER_AGENT, parseInt(env.MAX_SEC_RPS) || 10);
    const companies = await env.DB
      .prepare("SELECT * FROM tracked_companies WHERE enabled = 1")
      .all<TrackedCompany>();

    let totalNew = 0;
    const tickers: string[] = [];

    for (const company of companies.results || []) {
      const { newFilings } = await scanCompany(env.DB, edgar, company, env.AI, env.CACHE);
      if (newFilings > 0) {
        totalNew += newFilings;
        tickers.push(`${company.ticker}:${newFilings}`);
      }
    }

    if (totalNew > 0) {
      console.log(`Cron: Found ${totalNew} new filings — ${tickers.join(", ")}`);

      // Report to Brain
      try {
        await env.BRAIN.fetch("https://brain/ingest", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            instance_id: "echo-sec-scraper",
            role: "assistant",
            content: `SEC CRON: ${totalNew} new filings detected — ${tickers.join(", ")}`,
            importance: 5,
            tags: ["sec", "cron", "filings"],
          }),
        });
      } catch (e) { log("warn", "Shared Brain ingest failed after cron scan", { error: (e as Error)?.message || String(e) }); }
    } else {
      console.log("Cron: No new filings found");
    }
  }
}

// ---------------------------------------------------------------------------
// Export
// ---------------------------------------------------------------------------

export default {
  fetch: app.fetch,
  scheduled: handleCron,
};
