# TET-Project3-EMR

TET-Project3-EMR is an academic AWS cloud deployment that implements a scalable big data pipeline for ingesting, processing, analyzing, and visualizing meteorological and traffic data.

## Authors
- Sara Cortes: svcortesm@eafit.edu.co
- Kristian Restrepo: krestrepoo@eafit.edu.co
- Evelyn Zapata: eazapatat@eafit.edu.co

## General Workflow Overview
This project is structured into three main layers: Ingestion, Processing, and Visualization. Each layer contains automated components designed to move data from raw to actionable insights efficiently and scalably.

The architecture connects ingestion, processing, and visualization layers in a modular and automated flow. This design ensures efficient data handling from raw capture to actionable insights delivered via RESTful APIs.
For a visual representation, visit the [Diagrams](https://github.com/SaraCmque/TET-Project3-EMR/wiki/Diagrams) page.

### 1. Ingestion Layer
This layer handles the initial capture and staging of raw data.

#### Steps
**1.** EventBridge triggers a Lambda function daily at 18:48 UTC using a cron rule.  
**2.** The Lambda fetches data from the Open-Meteo API and RDS (365 days of historical traffic data).  
**3.** Data is uploaded to the S3 bucket under the `raw/` folder.  
**4.** S3 triggers a processing Lambda when new files appear under `raw/`.  
**5.** The Lambda checks a lock in DynamoDB to prevent concurrent EMR cluster launches.  
**6.** If no lock is found, it initiates an EMR cluster to process the data.  
**7.** S3 lifecycle policies delete files in `raw/`, `trusted/`, and `refined/` after 1 day.

#### Used tools
- AWS EventBridge, Lambda, S3, RDS (MySQL), and DynamoDB.
- The S3 bucket has structured folders: `raw/`, `trusted/`, `refined/`, and `scripts/`.
- Bucket permissions include upload, delete, and read access, with AES-256 server-side encryption.
- DynamoDB manages the lock using a 2-minute TTL to avoid simultaneous executions.

> For further details, review [Ingest Layer Explanation](https://github.com/SaraCmque/TET-Project3-EMR/wiki/Components-description#1-ingest-layer-explanation)

### 2. Processing Layer
This layer performs data cleaning, enrichment, exploratory analysis, and modeling using Spark on EMR.

#### Steps
**1.** Triggered by a Lambda function when a new file is added to `raw/` in the S3 bucket.  
**2.** Launches an EMR cluster with multiple steps: ETL, analysis, and modeling.  
**3.** ETL script `transform_to_trusted.py` extracts and cleans climate and traffic data.  
**4.** Climate data is normalized (e.g., exploding nested arrays), and traffic data is cleaned and joined by date.  
**5.** Saves processed data in Parquet format to `trusted/`.  
**6.** Bootstrap script `install.sh` installs required Python packages (pandas, seaborn, matplotlib, etc.).  
**7.** Analysis script `analysis.py` computes descriptive statistics and correlations using Spark.  
**8.** Generates visualizations (bar charts, heatmaps) with Pandas + Seaborn, stored as PNGs in `refined/visualizations/`.  
**9.** Metadata for visualizations is saved in `refined/visualizations_metadata/`.  
**10.** Modeling script `model.py` trains Random Forest and GBT regressors using Spark ML Pipelines.  
**11.** Feature vector includes weather and traffic variables (e.g., max_temp, precipitation, congestion_level_num).  
**12.** Generates and stores model evaluation results and predictions.  
**13.** Applies models to a synthetic 60-day dataset for Medellín to forecast accident counts.  
**14.** Final outputs are saved in `trusted/` (cleaned data) and `refined/` (EDA, visualizations, predictions, and metrics).  

#### Used tools
- AWS Lambda, S3, and EMR.
- DynamoDB lock (`EmrClusterLock`) prevents concurrent EMR jobs.

> For full details, review [Processing Layer Explanation](https://github.com/SaraCmque/TET-Project3-EMR/wiki/Components-description#2-processing-layer-explanation)

### 3. Visualization Layer
This layer enables access to refined data through query interfaces and APIs.

#### Steps
**1.** Spark jobs output results (descriptive stats, ML predictions) to S3 in Parquet format under `refined/`.  
**2.** Athena creates external tables over these Parquet files using `CREATE EXTERNAL TABLE`, with compression set to Snappy.  
**3.** Each table (e.g., `estadisticas_congestion`, `correlaciones`, `predictions_gbt`, etc.) points to a specific S3 path.  
**4.** Lambda function `project3-api-handler` maps HTTP paths (e.g., `/congestion-stats`) to SQL queries on Athena.  
**5.** Lambda uses `boto3` to start Athena queries, polls the status, and returns results as JSON.  
**6.** JSON responses include data and query execution ID; headers include CORS and `Content-Type: application/json`.  
**7.** API Gateway exposes REST endpoints (e.g., `/correlations`, `/visualizaciones`) linked to the Lambda function.  
**8.** Each endpoint uses the GET method, has CORS enabled, and integrates with Lambda for query execution.  
**9.** Clients can query API endpoints (e.g., via `curl`) to retrieve processed analytics in JSON format.  
**10.** Example queries return average incidents, temperature, and precipitation by congestion level.  

#### Used tools
- AWS Lambda, S3, Athena, and API Gateway.
- Lambda and Athena are fully decoupled; all queries reference S3-stored Parquet datasets via Athena external tables.

> For full details, review [Data Visualization Layer Explanation](http://github.com/SaraCmque/TET-Project3-EMR/wiki/Components-description#3-data-visualization-layer-explanation)

## Project Presentation
For a comprehensive video explanation of the project, please visit [Project Video Explanation](https://www.canva.com/design/DAGpD6jp_dM/09ZwufV7FVTVMP1f-vJQYQ/watch?utm_content=DAGpD6jp_dM&utm_campaign=designshare&utm_medium=link2&utm_source=uniquelinks&utlId=hab2cc7f00d)
