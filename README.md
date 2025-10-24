# NYC TAXI DATA PIPELINE


## Navigation / Quick Access
Quickly move to a section:
- [Overview](#overview)
- [Project Objective](#project-objective)
- [Project Architecture](#project-architecture)




---
## Overview
This project involves building an entire data pipeline in SQL that ingests, transforms, and aggregate. It demonstarate both Full refresh and Incremental load strategies into three layers of Medallion architecture:
- **Raw Layer (Bronze)**: Ingest data incrementally with metadata tracking from source
- **Transformed Layer (Silver)**: Full data loading from bronze layer: cleaning, transformation, normalization, and standardization
- **Aggregated Layer (Gold)**: Produces summarized and analytics ready data.


---
## Project Objective
The objective of this project is to design and implement a fully SQL-based ETL pipeline that automates data ingestion, transformation, and aggregation for the **NYC Taxi 2024 dataset**. 

The pipeline demonstrates both **full refresh** and **incremental loading** strategies within the Medallion Architecture framework, ensuring efficient, scalable, and idempotent data processing.

Specifically, the project aims to:
- Implement an **automated ingestion process** that loads monthly trip data incrementally into the Raw (Bronze) layer and full load into the Transformed (Silver) layer.
- Build a **metadata table** to track load history and prevent duplicate inserts.
- Transform raw data into standardized, cleaned formats in the **Silver** layer using partitioned tables.
- Aggregate and summarize data in the **Gold** layer for analytical and reporting purposes.
- Deliver comprehensive **SQL scripts** and a **pipeline architecture diagram** demonstrating data flow across all layers.

---
## Project Architecture
![pipeline architecture diagram](architecture_diagram.svg)
