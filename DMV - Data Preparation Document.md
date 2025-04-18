# DMV (Data Monitoring and Visualization System) Data Preparation Documentation

## 1. Introduction

**Purpose:**  
This document details the process and infrastructure for preparing data for visualizations in the DMV system. DMV runs on top of Apache Superset and connects to a unified database that aggregates raw data from multiple government agencies.

**Scope:**  
This documentation covers data ingestion from various agencies (e.g., MOW/RUWASA, MOH, PO-RALG), the ETL process that transforms raw data into metrics, and the final preparation of data for visual dashboards in DMV.

**Audience:**  
This document is intended for data engineers, database administrators, data analysts, and stakeholders involved in the DMV system.

---

## 2. Importance of Data Preparation for Business Intelligence (BI) Tools

Structured data preparation is essential for maintaining data accuracy, ensuring logical consistency, and facilitating efficient issue tracking. Without a standardized ETL process, errors in data ingestion, transformation, or aggregation can lead to misleading insights and unreliable decision-making. Key benefits include:

1. **Ensuring Data Accuracy:**
   - Captures anomalies early by validating source data against expected logic.
   - Reduces discrepancies by structuring and cleansing raw data before visualization.

2. **Exception Handling and Resolution Tracking:**
   - Identifies logical exceptions (e.g., missing values, unexpected data patterns) for quick intervention.
   - Enables a structured follow-up process to resolve inconsistencies.

3. **Reliable Business Metrics:**
   - Ensures that reported Indicators are derived from well-structured, validated data.
   - Reduces the risk of incorrect conclusions due to inconsistencies in underlying data.

4. **Optimized Query Performance:**
   - Pre-aggregated datasets reduce the computational load on BI tools.
   - Improves dashboard responsiveness and user experience.

5. **Scalability & Maintainability:**
   - Allows seamless data integration from multiple sources without compromising accuracy.
   - Facilitates debugging and future enhancements by maintaining a clear data lineage.

---

## 3. System Architecture Overview

**Components:**

- **Data Sources:**
  - **MOW/RUWASA, MOH, PO-RALG:** Provide raw data.
- **Unified Database:**
  - Aggregates raw data into a single repository.
- **Schemas:**
  - **public:** Stores raw data fetched via APIs from MOH and PO-RALG.
  - **foreign\_schema\_ruwasa\_rsdms:** Stores data directly from Ruwasa (RSDMS) – since DMV is hosted within RSDMS.
  - **visualizations:** Stores processed data and metrics that are ready for visualization.
- **ETL Processes:**
  - Implemented as PostgreSQL functions and triggers that transform data between the `public` and `foreign_schema_ruwasa_rsdms` schemas, producing aggregated metrics in the `visualizations` schema.
- **DMV (Data Monitoring and Visualization System):**
  - A tool built on top of Apache Superset that provides visual dashboards for monitoring key metrics.

---

## 4. Data Sources

**Agencies & Data Formats:**

- **MOH & PO-RALG:**
  - Data fetched via API. Stored in the **public** schema.
- **MOW/RUWASA:**
  - Data provided from `foreign_schema_ruwasa_rsdms` schema.

Data formats for raw data are guided by the API payload structure documentation defined for each agency.

---

## 5. Database Schemas and Their Roles

### **public Schema**

- **Purpose:**  
  Stores raw data fetched from MOH and PO-RALG via APIs.
- **Responsibilities:**  
  Maintain the original, untransformed data from source agencies.

### **foreign\_schema\_ruwasa\_rsdms**

- **Purpose:**  
  Stores data sourced directly from Ruwasa (RSDMS).
- **Responsibilities:**  
  Allow DMV to interface directly with RSDMS for MOW/RUWASA data access.

### **visualizations Schema**

- **Purpose:**  
  Stores processed and aggregated data (metrics) that are ready for visualization in DMV.
- **Responsibilities:**  
  Provide optimized, aggregated datasets for dashboards. Data is transformed and refreshed via scheduled ETL processes.

---


## 6. ETL Process Details

**ETL Tools & Technologies:**

- PostgreSQL functions and triggers perform the ETL.
- Scheduling is set up to fetch and transform data on a quarterly, yearly, or other defined basis.

**Data Flow:**

1. **Ingestion:**  
   - APIs fetch raw data into the **public** schema.
   - Data from RSDMS is ingested into **foreign\_schema\_ruwasa\_rsdms**.
2. **Transformation:**  
   - ETL functions/triggers join and aggregate raw data from the `public` and `foreign_schema_ruwasa_rsdms` schemas.
   - New metrics are calculated (e.g., percentage of functioning water points, access metrics).
3. **Storage for Visualization:**  
   - Transformed data is stored in the **visualizations** schema.
   - DMV queries these tables to create dynamic visualizations.

---

## 7. Detailed ETL Implementation Example

### **Example 1: Trigger-Based Data Refresh**

```sql
CREATE OR REPLACE FUNCTION visualizations.refresh_functioning_water_points()
RETURNS trigger AS $$
BEGIN
    TRUNCATE TABLE visualizations.functioning_water_points RESTART IDENTITY;
    
    INSERT INTO visualizations.functioning_water_points (year, region, percentage)
    SELECT DATE_TRUNC('year', date_recorded) AS year, region, AVG(functioning_percentage) AS percentage
    FROM public.water_data
    GROUP BY DATE_TRUNC('year', date_recorded), region;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_refresh_functioning_water_points ON public.water_data;
CREATE TRIGGER trg_refresh_functioning_water_points
AFTER INSERT OR UPDATE OR DELETE ON public.water_data
FOR EACH STATEMENT
EXECUTE FUNCTION visualizations.refresh_functioning_water_points();
```

---

### **Example 2: Scheduled Data Aggregation**

```sql
CREATE OR REPLACE FUNCTION visualizations.calculate_monthly_metrics()
RETURNS void AS $$
BEGIN
    TRUNCATE TABLE visualizations.monthly_metrics RESTART IDENTITY;
    
    INSERT INTO visualizations.monthly_metrics (month, metric_value)
    SELECT DATE_TRUNC('month', recorded_date) AS month, AVG(metric) AS metric_value
    FROM public.source_data
    GROUP BY DATE_TRUNC('month', recorded_date);
END;
$$ LANGUAGE plpgsql;
```

#### **Shell Script for Scheduling Execution**

```sh
#!/bin/bash
PGPASSWORD="your_password" psql -U your_user -d your_database -h your_host -c "SELECT visualizations.calculate_monthly_metrics();"
```

This script can be added as a cron job to run periodically:

```sh
crontab -e
```

Then add the following line to schedule it daily at midnight:

```sh
0 0 * * * /path/to/script.sh
```

---

