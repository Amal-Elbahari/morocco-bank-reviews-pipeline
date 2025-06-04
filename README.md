# Analyzing Customer Reviews of Bank Agencies in Morocco

This project aims to collect, transform, and analyze customer reviews from Google Maps about Moroccan bank agencies, using a **modern data stack** for end-to-end data processing.

## 📌 Objective

Extract insights from unstructured customer reviews to:
- Understand customer sentiment.
- Detect recurring topics and issues.
- Rank agency performance.
- Improve customer experience through data.

---

## 📐 Project Scope

Banks in Morocco receive thousands of reviews on Google Maps. These reviews contain valuable information but are unstructured and spread across locations. This project centralizes, cleans, and analyzes that data.

### ✨ Key Insights
- 📊 Sentiment trends across agencies
- 🔍 Topic modeling for complaints and praise
- 🏅 Performance ranking of branches
- ✅ Key drivers of satisfaction and dissatisfaction

---

## ⚙️ Tech Stack

| Stage            | Tools Used                              |
|------------------|------------------------------------------|
| Data Collection  | Python, Google Maps API, BeautifulSoup   |
| Scheduling       | Apache Airflow                          |
| Storage          | PostgreSQL                              |
| Transformation   | DBT (Data Build Tool)                   |
| Visualization    | Looker Studio                           |
| Version Control  | Git + GitHub                            |

---

## 🚀 Project Pipeline

### ✅ Phase 1: Data Collection
- Scraped reviews from Google Maps.
- Stored raw data in JSON/CSV.
- Automated with Apache Airflow.

### ✅ Phase 2: Data Cleaning & Enrichment
- Removed duplicates, normalized text.
- Applied sentiment analysis.
- Extracted topics with LDA.

### ✅ Phase 3: Data Modeling
- Star Schema in PostgreSQL:
  - `fact_reviews`
  - `dim_bank`
  - `dim_branch`
  - `dim_location`
  - `dim_sentiment`

### ✅ Phase 4: Visualization
- Built dashboards in Looker Studio:
  - Sentiment trends
  - Top positive/negative topics
  - Agency rankings

### ✅ Phase 5: Deployment
- Fully automated with Airflow
- Alerts for failures (future improvement)

---

## 📂 Repository Contents

| File/Folder       | Description                                     |
|------------------|-------------------------------------------------|
| `dags/`           | Airflow DAG for automated data pipeline        |
| `dw_reviews_project/`            | DBT models for staging, marts, and star schema |
| `scriptnew/`   |       Python script to collect, load, transform , enrich reviews               |
                         |
| `README.md`       | Project overview                               |

---

## 📈 Dashboard Preview
🔗 [View the Dashboard on Looker Studio](https://lookerstudio.google.com/reporting/2be8ed47-9988-4c17-8f00-341efec0d6b0)


---

## 🧪 How to Run the Project

1. Clone the repo:

```bash
git clone https://github.com/YOUR_USERNAME/morocco-bank-reviews-pipelinet.git
cd dw_reviews_project

