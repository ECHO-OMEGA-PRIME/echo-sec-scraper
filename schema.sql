-- echo-sec-scraper D1 Schema
-- Run: npx wrangler d1 execute echo-sec-scraper --remote --file=schema.sql

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

-- Seed default companies
INSERT OR IGNORE INTO tracked_companies (ticker, cik, name) VALUES ('AAPL', '0000320193', 'Apple Inc.');
INSERT OR IGNORE INTO tracked_companies (ticker, cik, name) VALUES ('MSFT', '0000789019', 'Microsoft Corporation');
INSERT OR IGNORE INTO tracked_companies (ticker, cik, name) VALUES ('GOOGL', '0001652044', 'Alphabet Inc.');
INSERT OR IGNORE INTO tracked_companies (ticker, cik, name) VALUES ('AMZN', '0001018724', 'Amazon.com Inc.');
INSERT OR IGNORE INTO tracked_companies (ticker, cik, name) VALUES ('NVDA', '0001045810', 'NVIDIA Corporation');
INSERT OR IGNORE INTO tracked_companies (ticker, cik, name) VALUES ('TSLA', '0001318605', 'Tesla Inc.');
INSERT OR IGNORE INTO tracked_companies (ticker, cik, name) VALUES ('META', '0001326801', 'Meta Platforms Inc.');
INSERT OR IGNORE INTO tracked_companies (ticker, cik, name) VALUES ('XOM', '0000034088', 'Exxon Mobil Corporation');
INSERT OR IGNORE INTO tracked_companies (ticker, cik, name) VALUES ('CVX', '0000093410', 'Chevron Corporation');
INSERT OR IGNORE INTO tracked_companies (ticker, cik, name) VALUES ('OXY', '0000797468', 'Occidental Petroleum Corporation');
