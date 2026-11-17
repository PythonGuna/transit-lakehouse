# Project Plan: Canadian Transit Reliability Lakehouse

> Master plan for the project. If anything gets confusing later, come back here.
> Update the Decisions Log (section 10) whenever a decision changes.

## 1. Goal

**Question the project answers:** How reliable is TTC service compared with its published schedule, by route, hour, and day?

**Why this project:** It shows real-time streaming, batch processing, cloud (Azure), and the Databricks, Snowflake, and Power BI stack in one end-to-end pipeline.

### Definition of done

- [ ] Poller runs continuously and archives raw data
- [ ] Streaming job produces live delay metrics
- [ ] Batch jobs build schedule-vs-actual tables
- [ ] Gold data is in Snowflake with dbt tests
- [ ] Power BI shows a live map and a reliability dashboard
- [ ] README, architecture diagram, and demo video are published
- [ ] Real numbers are recorded: rows processed, latency, monthly cost

## 2. Scope

| MVP (must ship) | Later (only if time allows) |
|---|---|
| TTC only | TransLink as a second agency |
| Vehicle positions and trip updates | Service alerts |
| Delay metrics and on-time performance | Late-arrival prediction |
| Minimal Terraform, one CI workflow | Full CI/CD, Unity Catalog, Data Factory |
| dbt tests | Great Expectations |

## 3. Architecture

**Streaming path:** Live feeds -> Python poller -> Event Hubs (Kafka endpoint) -> Spark Structured Streaming (Databricks) -> Delta tables.

**Batch path:** Static GTFS zips -> daily loader -> ADLS Gen2 -> Spark batch jobs (Bronze to Gold).

**Shared:** Delta lakehouse on ADLS Gen2 (Bronze, Silver, Gold) -> Snowflake (dbt models and tests) -> Power BI.

**Cross-cutting:** Terraform, GitHub Actions, Key Vault, Azure Monitor, data quality tests, Databricks Workflows.

## 4. Technology decisions

| Area | Choice |
|---|---|
| Language | Python 3.11+ |
| Cloud | Azure (Canadian region) |
| Ingestion | Python poller in a container (Azure Container Apps) |
| Streaming transport | Event Hubs via its Kafka endpoint (needs a tier above Basic; check current pricing) |
| Storage | ADLS Gen2, Delta format |
| Processing | Azure Databricks (PySpark, Structured Streaming) |
| Orchestration | Databricks Workflows |
| Warehouse | Snowflake with dbt |
| BI | Power BI |
| IaC and CI | Terraform, GitHub Actions |
| Secrets | `.env` locally (never committed), Key Vault in Azure |

## 5. Data model

### Bronze (raw, append-only)

- `raw_vehicle_positions`, `raw_trip_updates`
- Raw static GTFS tables
- Keep the original message plus `ingest_ts`, `feed_ts`, `agency`

### Silver (clean, typed, deduplicated)

- `vehicle_positions`: agency, vehicle_id, trip_id, route_id, lat, lon, bearing, speed, `event_ts`, `ingest_ts`. Deduplicate on (agency, vehicle_id, event_ts).
- `trip_updates`: trip_id, route_id, stop_id, stop_sequence, delay_sec, predicted_time, `event_ts`
- Static tables: `routes`, `stops`, `trips`, `stop_times`, versioned by load date

### Gold (business-ready)

- `fact_stop_arrival`: scheduled vs observed time and delay per trip and stop
- `agg_route_delay_5min`: streaming windowed metric
- `agg_route_otp_daily`: on-time percentage by route and day
- `dim_route`, `dim_stop`, `dim_date`

### Key modeling decisions

- **On time** means between 1 minute early and 5 minutes late. This is an adjustable assumption; document any change in the Decisions Log.
- **Observed delay** comes from the last trip update before a stop's scheduled time passes, not from inferring arrivals from GPS points.
- **Event time** uses the vehicle record timestamp. Keep ingestion time as a separate column.
- Nullable fields: occupancy, speed, and delay may be missing. Track completeness as a data quality metric.

## 6. Repository standards

**Repo:** `transit-lakehouse` (public on GitHub)

```
transit-lakehouse/
├── README.md
├── PLAN.md
├── docs/                   # architecture diagram, decisions, cost notes
├── infra/terraform/
├── ingestion/
│   ├── poller/
│   └── static_loader/
├── databricks/
│   ├── streaming/
│   ├── batch/
│   └── workflows/
├── dbt/
├── dashboards/
├── tests/
├── .github/workflows/
├── .env.example            # variable names only, no secrets
├── .gitignore
├── pyproject.toml
└── requirements.txt
```

### Working rules

- `main` always works. Use short-lived feature branches (for example `feat/poller`) merged by pull request.
- Commit style: `feat:`, `fix:`, `docs:`, `chore:`.
- No secrets in Git, ever. Commit `.env.example` only.
- Type hints, `ruff` for linting, `pytest` for tests, `pre-commit` to run them.
- Keep URLs and settings in config, not in code (agencies change endpoints).

## 7. Phases and milestones

| Phase | Dates | Tasks | Done when |
|---|---|---|---|
| 0. Setup | Nov 16 - 17 | Laptop tools, GitHub repo, folder structure, starter files | Repo pushed to GitHub |
| 1. Collect | Nov 17 - 22 | Poller (local first, then container), Azure account and budget alert, Event Hubs, raw archive to ADLS, static GTFS loader | Raw data accumulating daily |
| 2. Pipelines | Nov 23 - 29 | Bronze and Silver (streaming and batch), dedupe, event time, watermarks | Silver tables populated; README updated |
| 3. Serve | Nov 30 - Dec 6 | Gold tables, Snowflake, dbt models and tests, Power BI, scheduled jobs | Dashboard works end to end |
| 4. Package | Dec 7 - 13 | Diagram, demo video, Terraform, CI workflow, resume numbers | Repo is presentable and shareable |

**Priority rule:** Protect Phases 1 and 2 above everything else. A working pipeline on GitHub by Nov 29 is enough to talk about in interviews, and the rest can be finished alongside them.

## 8. Cost and risk guardrails

### Cost

- Build and test locally first; it costs nothing.
- Create the Azure account only when the poller is ready to deploy. The free account's $200 credit lasts 30 days from signup.
- Set a budget alert the same day the account is created.
- Smallest Databricks cluster with auto-terminate. Stop Event Hubs and the container when not collecting or demoing.
- Track weekly cost in `docs/cost.md`.
- Check current Azure, Databricks, and Snowflake pricing before deploying.

### Risks

| Risk | Mitigation |
|---|---|
| Feed endpoint or terms change | URLs in config; check TTC open data terms; log every poll's status |
| Missing fields in feed | Nullable columns and a completeness metric |
| Runaway cloud cost | Budget alerts, auto-terminate, teardown routine |
| Scope creep | Stick to the MVP list in section 2 |
| Time pressure | Push to GitHub by end of Phase 2 and apply with the project marked "in progress" |

## 9. Log fields for every poll (pipeline health)

HTTP status, response size, feed header timestamp, entity count, decode errors. These become a monitoring panel.

## 10. Decisions log

| Date | Decision | Reason |
|---|---|---|
| Nov 2026 | Transit (GTFS-Realtime) as dataset | Live data, local relevance, less common than taxi data |
| Nov 2026 | Azure as cloud | Fits Databricks and Power BI background |
| Nov 2026 | TTC first, TransLink later | TTC needs no API key (confirm current terms) |
| Nov 2026 | Local-first development | Avoids cloud cost until needed |
| Nov 2026 | On-time = 1 min early to 5 min late | Simple, adjustable assumption |

## 11. Resume line (update as things become true)

**Canadian Transit Reliability Lakehouse (in progress)** | Azure, Databricks, Event Hubs, Spark Structured Streaming, Snowflake, dbt, Power BI

Only add claims and numbers that are actually true and measured.
