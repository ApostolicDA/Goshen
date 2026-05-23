# Goshen Global Church — Analytics Platform

> *"What gets measured gets managed. What gets understood gets transformed."*

A full-stack analytics platform built for a real client — ingesting social media data from 4 sources, modelling it through a production-grade dbt pipeline, and surfacing actionable insights in a multi-page Looker Studio dashboard that drives real content decisions.


---

## 🔥 The Problem

Goshen Global Church had no idea what was working online.

They were posting content across Facebook, YouTube, and TikTok — week after week — with zero visibility into what the data was saying. No idea which platform was driving reach. No idea what day to post. No idea why one TikTok video exploded to 100K+ views while others barely moved.

Leadership was making content decisions based on gut feeling.

**I built them the infrastructure to make decisions based on data.**

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│  Facebook Graph API  │  YouTube Data API  │  TikTok CSV Export  │
└──────────────────────┴────────────────────┴─────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                     INGESTION LAYER                             │
│              Python scripts → Snowflake raw tables              │
│         Orchestrated via Airflow DAGs (cloud-ready)             │
│         Scheduled locally via Windows Task Scheduler            │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                   TRANSFORMATION LAYER                          │
│                        dbt Core                                 │
│   Staging models (views) → Mart models (tables)                 │
│   22 models │ 84 data tests │ 11 marts                          │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PRESENTATION LAYER                           │
│                  Looker Studio Dashboard                        │
│   5 pages │ Cross-platform analytics │ Actionable insights      │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Data Warehouse | Snowflake |
| Transformation | dbt Core 1.11 |
| Ingestion | Python 3.11 |
| Orchestration | Apache Airflow (DAGs) + Windows Task Scheduler |
| Visualization | Looker Studio |
| Version Control | Git + GitHub |
| APIs | Facebook Graph API, YouTube Data API v3, TikTok CSV |
| Environment | Docker-ready (WSL2 architecture designed) |

---

## 📊 Dashboard Pages

### 1. Goshen Social Overview
Cross-platform command centre. 3.8K total community. 27.3K total views. One glance tells the full story.

### 2. Facebook Analytics
- 1.1K followers │ 4.6K impressions │ 565 page views
- Thursday & Saturday: peak organic reach (1.3K impressions each)
- Wednesday: high page visits, zero content — **the biggest missed opportunity in the data**

### 3. YouTube Analysis
- 61 videos │ 6K views │ **23% engagement rate** — highest across all platforms
- Short content under 20 min drives 70% of all views
- Tuesday is peak viewing day — post-Sunday intentional traffic

### 4. TikTok Live Analytics
- 2.6K followers │ 20.2K views │ **300.2K likes**
- Long live streams dominate — Sunday services are the #1 content
- November 2025: viral moment — 100K+ views, 5x follower growth in 4 months

### 5. TikTok Posts Analytics
- 75 posts │ 13.2K likes │ 8.6K highest post
- Only 9 posts exceed 100 likes — channel is carried by live streams and one viral moment
- Orlando YMCA location tag → highest performing content location
- Original sound beats trending audio every time

---

## 🔍 Key Insights Surfaced

These aren't just charts. These are decisions.

**1. The Wednesday Content Desert**
Wednesday has some of the highest page view traffic of the week — people coming to check midweek service times — but near-zero reactions because there's no fresh content to engage with. One post per Wednesday would convert that passive traffic into engagement.

**2. The Thursday/Saturday Algorithm Window**
Facebook and YouTube both peak on Thursday and Saturday for organic reach. The algorithm pushes hardest on these days. Post on Thursday, post on Saturday. Everything else is downstream.

**3. The November 2025 Blueprint**
One TikTok post in November 2025 generated 100K+ views, 8K likes, and triggered 5x follower growth. It was filmed at Orlando YMCA with original sound — no music, no edits, just authentic content. That's the formula.

**4. TikTok is a Different League**
TikTok likes (300.2K) dwarf Facebook (75) and YouTube (702) combined. The church's digital ministry lives on TikTok. Everything else supports it.

**5. YouTube's Silent Strength**
126 subscribers generating 23% engagement rate. Small but deeply engaged audience. Short clips under 20 minutes drive 70% of views — the sermon archive strategy is working.

---

## 📁 Project Structure

```
goshen/
├── models/
│   ├── staging/          # 10 staging views (one per source table)
│   │   ├── stg_facebook_insights.sql
│   │   ├── stg_facebook_posts.sql
│   │   ├── stg_tiktok_followers.sql
│   │   ├── stg_tiktok_live.sql
│   │   ├── stg_tiktok_posts.sql
│   │   ├── stg_youtube_channel.sql
│   │   ├── stg_youtube_videos.sql
│   │   └── schema.yml
│   └── marts/            # 11 mart tables (business-ready)
│       ├── mart_facebook_insights.sql
│       ├── mart_facebook_metrics.sql
│       ├── mart_facebook_posts.sql
│       ├── mart_posts_perfomance.sql
│       ├── mart_social_followers_snapshot.sql
│       ├── mart_social_overview.sql
│       ├── mart_tiktok_activity_overlap.sql
│       ├── mart_tiktok_followers.sql
│       ├── mart_tiktok_live_perfomance.sql
│       ├── mart_youtube_perfomance.sql
│       ├── mart_youtube_videos.sql
│       └── schema.yml    # 84 data tests
├── Dags/                 # Airflow DAGs (cloud deployment ready)
│   ├── dag_master_pipeline.py
│   ├── dag_facebook_pipeline.py
│   ├── dag_youtube_pipeline.py
│   ├── dag_tiktok_pipeline.py
│   └── dag_dbt_run.py
├── ingestion/            # Python ingestion scripts
│   └── run_ingestion.py
├── run_pipeline.bat      # Local automation script
├── packages.yml          # dbt packages (dbt_utils)
└── dbt_project.yml
```

---

## ⚙️ Pipeline Automation

### Local (Active)
Pipeline runs daily via Windows Task Scheduler:
```
00:00 SAST → run_pipeline.bat triggers
           → Python ingestion scripts pull from APIs
           → dbt run rebuilds 22 models
           → dbt test validates 84 tests
           → Looker Studio reflects fresh data
```

### Cloud (Airflow DAGs — Ready for Deployment)
Full Airflow orchestration designed and documented in `/Dags`:
- **dag_master_pipeline.py** — parallel ingestion trigger + dbt orchestration
- **dag_facebook_pipeline.py** — Facebook Graph API ingestion + validation
- **dag_youtube_pipeline.py** — YouTube Data API v3 ingestion
- **dag_tiktok_pipeline.py** — TikTok CSV processing + archiving
- **dag_dbt_run.py** — Staged dbt execution (staging → marts → docs)

Deployment target: AWS EC2 or GCP Compute Engine (pending cloud budget).

---

## 🧪 Data Quality

84 dbt tests across all 11 marts:

| Test Type | Count | Purpose |
|-----------|-------|---------|
| `not_null` | 42 | Critical fields never empty |
| `unique` | 11 | Grain integrity per mart |
| `accepted_values` | 7 | Day of week validation across all models |
| `expression_is_true` | 24 | Numeric fields never negative |

**Result: PASS=82 WARN=2 ERROR=0**

The 2 warnings are documented known limitations:
- `metric_value` nulls: Meta API returns null for days with zero activity (expected)
- `total_views` nulls: Not all platforms expose views in the snapshot endpoint (expected)

---

## 📈 Impact

This platform gave Goshen Global Church:

- **A content calendar backed by data** — not guesswork
- **Cross-platform visibility** in one dashboard
- **Identification of the Wednesday opportunity** — high traffic, zero content
- **The viral content blueprint** — Orlando YMCA + original sound = reach
- **Proof that TikTok live streaming is the primary growth engine**

> *"Goshen Global Church provided a formal recommendation letter upon project completion."*

---

## 🚀 Running Locally

```bash
# Clone the repo
git clone https://github.com/ApostolicDA/Goshen.git
cd Goshen

# Create virtual environment
python -m venv goshen-dbt-env
source goshen-dbt-env/Scripts/activate  # Windows: activate.bat

# Install dependencies
pip install dbt-bigquery dbt-utils snowflake-connector-python python-dotenv

# Configure environment variables
cp .env.example .env
# Fill in your API keys and Snowflake credentials

# Run the pipeline
python run_ingestion.py
dbt run
dbt test
```

---

## 👤 Built By

**Proud Kudzai Ndlovu**
Data & Analytics Engineer │ Johannesburg, South Africa
Remote contracts │ UTC+2

- 📧 fanisaproud@gmail.com
- 💼 [LinkedIn](https://linkedin.com/in/) 
- 🐙 [GitHub](https://github.com/ApostolicDA)

*Stack: dbt · Snowflake · BigQuery · Airflow · Python · SQL · Power BI · Looker Studio*

---

> Built with engineering discipline, real client data, and the conviction that every organisation — regardless of size — deserves to understand their data.
