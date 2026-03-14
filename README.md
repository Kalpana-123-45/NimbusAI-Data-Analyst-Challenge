# 🚀 NimbusAI — Data Analyst Intern Challenge
### Focus Area: Option A — Customer Churn & Retention Analysis

> **Submitted by:** Data Analyst Candidate  
> **Challenge:** RoaDo AI · Data Analyst Intern Take-Home  
> **Databases:** PostgreSQL (`nimbus_core`) + MongoDB (`nimbus_events`)  
> **Customers Analysed:** 1,204 · **Churn Rate Found:** 22.1%

---

## 📁 Repository Structure

```
NimbusAI-Data-Analyst-Challenge/
│
├── 📄 README.md                    ← You are here
├── 📊 dashboard_app.html           ← Task 4: Interactive Dashboard (open in browser)
│
├── 🗄️  nimbus_core.sql             ← Raw Data: PostgreSQL dump
├── 🍃 nimbus_events.js             ← Raw Data: MongoDB dump
│
├── 📝 task_1_sql_queries.sql       ← Task 1: 5 Advanced SQL Queries
├── 📝 task_2_mongo_queries.js      ← Task 2: 4 MongoDB Aggregation Pipelines
├── 🐍 task_3_analysis.py           ← Task 3: Data Wrangling + Stats + Segmentation
│
└── 📦 requirements.txt             ← Python dependencies
```

---

## 🎯 The Business Problem

NimbusAI is a B2B SaaS company with 1,204 customers. Leadership flagged three concerns:
- Churn is ticking up
- Support ticket volume is rising
- Debate over which features to invest in next quarter

My job: dig into **PostgreSQL** (customers, subscriptions, billing, tickets) and **MongoDB** (activity logs, onboarding events) and come back with answers, not guesses.

---

## 📊 Task 4 — Interactive Dashboard

**➡️ Open `dashboard_app.html` directly in your browser — no installation needed.**

Pure HTML + Chart.js. Works completely offline.

### What's inside:

| # | Visualisation | Data Source | Business Question |
|---|---|---|---|
| 1 | MRR & New Subscriptions trend | SQL | Is revenue growing? Where did it drop? |
| 2 | Churn Rate by Signup Source | SQL | Which acquisition channel retains best? |
| 3 | Churn Rate by Industry | SQL | Which verticals are highest risk? |
| 4 | Customer Segment Donut | SQL + MongoDB | How is our customer base distributed? |
| 5 | Onboarding Funnel Drop-off | MongoDB | Where are users abandoning onboarding? |

**2 Interactive Filters** — Industry + Signup Source (KPI cards update live when you click)

---

## 🗃️ Task 1 — SQL Queries (`task_1_sql_queries.sql`)

Five production-grade PostgreSQL queries with full comments:

| Query | Technique | What it answers |
|---|---|---|
| Q1 | JOINs + Aggregation | Support ticket rate & avg MRR per plan tier (last 6 months) |
| Q2 | Window Functions | Customer LTV rank within tier + % diff from tier average |
| Q3 | CTEs + Subqueries | Customers who downgraded AND had >3 tickets in 30 days prior |
| Q4 | Time Series | MoM subscription growth + rolling 3-month churn flag |
| Q5 | Advanced Dedup Logic | Detect duplicate accounts via email domain + name + team members |

---

## 🍃 Task 2 — MongoDB Queries (`task_2_mongo_queries.js`)

Four aggregation pipelines that handle real-world messiness — mixed field names, inconsistent timestamps, orphan IDs:

| Pipeline | What it answers |
|---|---|
| Q1 | Avg sessions/user/week + P25/P50/P75 session duration by tier |
| Q2 | DAU + 7-day retention rate per product feature |
| Q3 | Onboarding funnel drop-off rates + avg time between steps |
| Q4 | Top 20 free-tier upsell targets with weighted engagement score |

---

## 🐍 Task 3 — Analysis Script (`task_3_analysis.py`)

```bash
pip install pandas numpy scipy scikit-learn matplotlib seaborn
python task_3_analysis.py
```

### Data Wrangling — Every cleaning step documented

| Step | Issue Found | Action Taken | Impact |
|---|---|---|---|
| Field normalisation | `userId` / `userID` / `member_id` — 3 different keys in MongoDB | Unified to single `member_id` | 0 orphaned joins |
| Timestamp parsing | 5 different formats (ISODate, US MM/DD, ISO+TZ, space-separated) | Parsed all → UTC | Consistent time analysis |
| Duplicate events | 187 duplicate records (same member + timestamp + event_type) | Dropped | 6,405 → 6,218 rows |
| Orphan records | Events with no resolvable customer ID | Dropped | Clean merge |
| Invalid NPS | Scores outside 0–10 range | Set to NaN | No skewed averages |
| Missing MRR | Null MRR in subscriptions table | Imputed from plan price | Complete revenue picture |

---

### Hypothesis Test

> *Do customers who use product features churn less?*

| | |
|---|---|
| **H₀** | Churn rate is the same for feature users and non-feature users |
| **H₁** | Feature users have a lower churn rate |
| **Test chosen** | Chi-Squared test of independence (2×2 contingency table) |
| **Why Chi-Squared** | Two categorical groups, comparing proportions — assumptions met (all expected cells ≥ 5) |
| **Significance level** | α = 0.05 |
| **Result** | χ² = 0.046, **p = 0.831** → Fail to reject H₀ |

**Interpretation:** No statistically significant difference. Feature users churn at 22.4% vs non-users at 21.7%. Churn is driven by **structural factors** — pricing, support quality, onboarding gaps — not feature adoption alone. This reframes where the product team should invest.

---

### Customer Segmentation — K-Means (k=4)

Features used: LTV, tenure, total events, distinct features used, MRR

| Segment | Customers | Avg LTV | Churn Rate | Avg Tenure | Recommended Action |
|---|---|---|---|---|---|
| 🏆 Champions | 7 | $2,121 | 14.3% | 688 days | Protect + upsell + get testimonials |
| 💚 Engaged | 422 | $1,783 | 22.3% | 928 days | Nurture with feature education |
| 🔴 At-Risk | 316 | $1,771 | 23.1% | 660 days | Proactive CS outreach now |
| 🔵 New/Low | 459 | $1,579 | 21.4% | 411 days | Improve onboarding flow |

---

## 💡 Top 3 Recommendations

### 1. 🔴 Target the At-Risk Segment — Highest ROI Action
316 customers sit in the At-Risk segment with a 23.1% churn rate and elevated ticket volume. Assign dedicated CS managers to any account with >3 support tickets in the past 30 days. Estimated recoverable ARR: **~$130K**.

### 2. 🚪 Plug the Onboarding Leak — Invite Teammate Step
MongoDB onboarding funnel shows **68% drop-off** at "Invite Teammate." Solo users churn significantly faster — team adoption is the stickiness driver. Fix: add a guided in-app invite prompt triggered immediately after first project creation.

### 3. 📢 Reallocate Acquisition Budget — Events → Partner Channel
Partner-acquired customers churn at **15.1%** vs event-acquired at **26.6%** — an **11.5 percentage point gap**, the largest signal in the entire dataset. Moving even 20% of event marketing spend to partner co-marketing directly improves LTV without touching the product.

---

## ⚙️ How to Run

```bash
# 1. Clone
git clone https://github.com/YOUR_USERNAME/NimbusAI-Data-Analyst-Challenge.git
cd NimbusAI-Data-Analyst-Challenge

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run analysis (generates CSVs + summary charts)
python task_3_analysis.py

# 4. Open dashboard — just double-click the file
open dashboard_app.html
```

---

## 🛠️ Tech Stack

| Layer | Tool |
|---|---|
| Relational DB | PostgreSQL (parsed from SQL dump) |
| NoSQL DB | MongoDB (parsed from JS dump) |
| Data Wrangling | Python · pandas · numpy |
| Statistics | scipy · Chi-Squared test |
| Segmentation | scikit-learn · K-Means |
| Dashboard | HTML5 · Chart.js · Vanilla JS |

---

*Submitted as part of the RoaDo AI Data Analyst Intern take-home challenge.*
