# Cereals Data Engineering Project
## 📌 Purpose
This project is part of my Data Engineering learning journey, transitioning from Food Technology to Data Engineering.  
The goal is to design a complete **end-to-end ETL pipeline** using a Kaggle dataset on breakfast cereals — from raw CSV data to a clean, queryable SQL database and API layer.
## 📊 Dataset
**Source:** [Kaggle - Cereal Dataset](https://www.kaggle.com/crawford/80-cereals)

**File Used:**  
- `cereal.csv` — contains details about 77 cereal brands including calories, protein, fat, vitamins, shelf placement, rating, and more.

## 🏗️ Architecture Overview

```plaintext
        +-------------------+
        |   CSV Dataset     |
        +---------+---------+
                  |
                  v
        +---------+---------+
        |   Python (ETL)    |
        | - Pandas          |
        | - Data Cleaning   |
        +---------+---------+
                  |
                  v
        +---------+---------+
        |   MySQL Database  |
        | - Normalized Tables|
        +---------+---------+
                  |
                  v
        +---------+---------+
        | FastAPI (optional)|
        | - Query endpoints |
        +-------------------+
