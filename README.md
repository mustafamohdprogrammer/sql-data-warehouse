# Data Warehouse and Analytics Project

Welcome to the **Data Warehouse and Analytics Project** repository! 🚀  
This project demonstrates a complete **data warehousing and analytics solution**, from building a data warehouse to delivering actionable insights. Designed as a portfolio project, it highlights **industry best practices** in data engineering and analytics.

---

## 🏗️ Data Architecture

This project follows the **Medallion Architecture** with **Bronze**, **Silver**, and **Gold** layers:

- **Bronze Layer**: Stores raw data exactly as received from the source systems. Data is ingested from CSV files into SQL Server.
- **Silver Layer**: Cleanses, standardizes, and normalizes data to make it analysis-ready.
- **Gold Layer**: Contains business-ready data modeled into a **Star Schema** for reporting and analytics.

---

## 📖 Project Overview

This project involves:

- **Data Architecture**: Designing a modern data warehouse using the Medallion Architecture.
- **ETL Pipelines**: Extracting, transforming, and loading data from sources into the warehouse.
- **Data Modeling**: Creating fact and dimension tables optimized for analytical queries.
- **Analytics & Reporting**: Developing SQL-based reports and dashboards for actionable insights.

---

## 🎯 Skills & Roles Covered

This project demonstrates skills in:

- SQL Development  
- Data Architecture  
- Data Engineering  
- ETL Pipeline Development  
- Data Modeling  
- Data Analytics  

---

## 🛠️ Tools & Resources

Everything in this project is **free to use**:

- **Datasets**: ERP and CRM CSV datasets.  
- **SQL Server Express**: Lightweight SQL database hosting.  
- **SQL Server Management Studio (SSMS)**: Manage and query databases.  
- **GitHub**: Version control and project collaboration.  
- **DrawIO**: Create architecture diagrams, data flows, and models.  
- **Notion**: Project management template and steps.

---

## 🚀 Project Requirements

### 1. Building the Data Warehouse (Data Engineering)
**Objective:**  
Develop a SQL Server-based data warehouse to consolidate sales data for analytical reporting.

**Specifications:**
- Import data from **two source systems** (ERP & CRM) as CSV files.
- Cleanse and resolve data quality issues.
- Integrate both sources into a single, user-friendly analytical model.
- Focus on the latest dataset only (no historization required).
- Provide **clear documentation** for business and analytics teams.

---

### 2. BI: Analytics & Reporting (Data Analysis)
**Objective:**  
Create SQL-based analytics delivering insights on:
- Customer behavior  
- Product performance  
- Sales trends  

These insights help stakeholders make **data-driven strategic decisions**.

---

## 📂 Repository Structure

```
data-warehouse-project/
│
├── datasets/                           # Raw datasets (ERP & CRM data)
│
├── docs/                               # Documentation & architecture files
│   ├── etl.drawio                      # ETL techniques and flow
│   ├── data_architecture.drawio        # Project architecture diagram
│   ├── data_catalog.md                 # Dataset field descriptions
│   ├── data_flow.drawio                # Data flow diagram
│   ├── data_models.drawio              # Star schema data model
│   ├── naming-conventions.md           # Table, column, file naming rules
│
├── scripts/                            # SQL scripts for ETL & transformations
│   ├── bronze/                         # Raw data ingestion scripts
│   ├── silver/                         # Data cleansing & transformation
│   ├── gold/                           # Analytical model creation
│
├── tests/                              # Test scripts and data quality checks
│
├── README.md                           # Project overview and instructions
├── LICENSE                             # License info
├── .gitignore                          # Ignored files for Git
└── requirements.txt                    # Project dependencies
```

---

## ☕ Stay Connected

Let’s connect and grow together! You can find me on:  
**[YouTube](#)** | **[LinkedIn](#)** | **[Website](#)** | **[Newsletter](#)** | **[Support Me](#)**  

---

## 🛡️ License

This project is licensed under the **MIT License** — you are free to use, modify, and share it with proper attribution.

---

## 🌟 About Me

Hi there! 👋 I’m an IT professional passionate about **data engineering, analytics, and sharing knowledge**. My goal is to make learning data fun and practical through real-world projects.  

**Let’s connect and collaborate!**
