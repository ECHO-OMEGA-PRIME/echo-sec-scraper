# echo-sec-scraper

> SEC EDGAR filing scraper with AI summarization, insider trade tracking, and financial intelligence digests.

## Overview

Echo SEC Scraper monitors SEC EDGAR for new filings from tracked public companies, parses filing metadata, extracts insider trading data from Form 4 filings, and uses Workers AI to generate summaries and key findings. All data is stored in D1 with structured queries for filings, insider trades, and AI-generated digests. The SEC EDGAR API is free and requires no authentication — only a User-Agent header.

Ships with 10 default tracked companies: AAPL, MSFT, GOOGL, AMZN, NVDA, TSLA, META, XOM, CVX, and OXY. Automatically tracks 10-K, 10-Q, 8-K, and Form 4 filings. Includes a built-in rate limiter capped at 10 requests/second to comply with SEC fair access policies.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check with company count, filing totals, and 24h filing count |
| `GET` | `/stats` | Detailed stats: tracked companies, filing type breakdown, insider trade count, digest count |
| `GET` | `/companies` | List all tracked companies with filing types and scan metadata |
| `POST` | `/companies/add` | Add a company to track. Body: `{ticker, cik?, name?, filing_types?[]}`. Auto-resolves CIK from SEC if not provided. |
| `DELETE` | `/companies/:id` | Remove a tracked company and all its filings |
| `POST` | `/scan` | Trigger a full scan of all enabled companies against SEC EDGAR |
| `GET` | `/filings` | Query filings with filters: `ticker`, `type`, `from`, `to`, `limit`, `offset` |
| `GET` | `/filings/recent` | Most recent filings across all companies. Query: `limit` |
| `GET` | `/filings/:accession` | Full filing detail by accession number |
| `POST` | `/filings/:accession/analyze` | Trigger AI analysis on a specific filing (summary + key findings) |
| `GET` | `/insider` | Query insider trades. Filters: `ticker`, `type`, `from`, `to`, `limit` |
| `GET` | `/digest` | Retrieve the most recent filing digest |
| `POST` | `/digest/generate` | Generate an AI-powered filing digest from recent filings |
| `POST` | `/init` | Manually initialize database schema and seed default companies |
| `GET` | `/` | Service info and endpoint listing |

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ENVIRONMENT` | `production` | Runtime environment label |
| `USER_AGENT` | `Echo Prime Technologies bobbymcwilliams@echo-op.com` | Required User-Agent header for SEC EDGAR API |
| `MAX_SEC_RPS` | `10` | Maximum requests per second to SEC endpoints |

### Bindings

| Binding | Type | Service/Resource |
|---------|------|------------------|
| `DB` | D1 Database | `echo-sec-scraper` — companies, filings, insider trades, digests |
| `CACHE` | KV Namespace | Hot cache for scan results and CIK lookups |
| `AI` | Workers AI | Llama 3.1 8B for filing summarization and digest generation |
| `BRAIN` | Service Binding | `echo-shared-brain` — broadcasts significant filings and digests |
| `KNOWLEDGE` | Service Binding | `echo-knowledge-forge` — knowledge integration |
| `SWARM` | Service Binding | `echo-swarm-brain` — MoltBook posting |

### Cron Triggers

| Schedule | Description |
|----------|-------------|
| `0 */6 * * *` | Scan all tracked companies every 6 hours |
| `0 6 * * *` | Daily filing scan at 06:00 UTC (1am CST) |

## Deployment

```bash
cd O:\ECHO_OMEGA_PRIME\WORKERS\echo-sec-scraper
npx wrangler deploy
```

## Architecture

Built on Hono with CORS. Uses the SEC EDGAR Submissions API (`data.sec.gov/submissions/CIK{cik}.json`) to fetch recent filings per company. A custom `RateLimiter` class enforces per-second request caps to comply with SEC fair access rules. Filing metadata is parsed from EDGAR's JSON response format, deduplicated by accession number, and stored in D1. Form 4 (insider trading) filings trigger additional parsing to extract transaction details (shares, price, total value, ownership changes). AI analysis via Workers AI generates summaries and key findings for individual filings. The D1 schema includes 4 tables: `tracked_companies`, `filings`, `insider_trades`, and `filing_digests` with indexes on company, filing type, date, and accession number.
