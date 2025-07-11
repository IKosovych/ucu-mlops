# 🛠 MLOps Pipeline with Airflow, Scraper, and MLflow

## 📌 Overview

This project implements a complete MLOps pipeline with the following components:

1. **Scraper**: Collects articles from the MSN News API.
2. **Processor**: Cleans and deduplicates the scraped data.
3. **Airflow DAG**: Orchestrates scraping and processing tasks automatically.
4. **Model**: Trains a text classifier to predict article categories.
5. **MLflow**: Logs training metrics and tracks model versions.

---

## ⚙️ Airflow

An Airflow DAG is used to orchestrate the scraping and processing steps:
- `scraper` task collects raw news articles and saves them as CSV to Google Cloud Storage (GCS).
- `processor` task cleans the data (removes duplicates, filters columns) and saves the cleaned CSV to GCS.

The output filenames include timestamps and are set dynamically using environment variables.

---

## 📰 Scraper

The scraper uses the MSN API to gather recent stories. Each record includes:
- `id`, `title`, `url`, `category`, `publishedDateTime`

The scraper saves the results to a CSV file and uploads it to GCS using the `upload_df_to_gcs()` function from the Google Cloud Storage SDK.

---

## 🧹 Processor

The processor script:
- Downloads the raw CSV file from GCS
- Cleans and deduplicates the data
- Saves the cleaned file back to GCS

---

## 🤖 ML Model (with MLflow)

The model is trained on the cleaned dataset using:
- `TfidfVectorizer` for feature extraction
- `LogisticRegression` for classification

The script logs:
- Accuracy score
- Classification report

Using **MLflow**, the experiment is tracked locally. Metrics and parameters are logged for future comparison.

---

## 🧪 Running the Model

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
