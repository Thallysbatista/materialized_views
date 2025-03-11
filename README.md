# BI Code Samples Portfolio

This repository is a collection of BI code samples that showcase my skills in SQL on Redshift and data analysis. The projects included demonstrate real-world business intelligence scenarios, with a strong focus on clean, well-documented code and best practices.

## Repository Structure

## Overview of Projects

- **SQL Projects:**  
  The SQL folder contains various examples of queries and views. For instance, the file `mvw_followup_responsible_by_sector.sql` creates a materialized view that tracks changes in followup sectors and identifies the person responsible for each change. All SQL scripts are fully commented in English and follow a strict formatting style (including placing commas to the left of the column names).

- `mvw_followups_sector_sla.sql`: Calculates how long each sale (`followup_id`) spent in each sector (e.g., SALES, FORMALIZATION, etc.). It helps identify delays or bottlenecks in the pipeline and can be used to compute SLA metrics like average, median, or percentiles.

- `mvw_user_first_vs_repeat_purchase.sql`: Identifies all completed purchases by user and classifies each one as a first-time or repeat purchase. Uses row numbering (`ROW_NUMBER()`) to flag the first sale chronologically per user based on their `followup_id` (which represents each individual sale).

- `mvw_monthly_growth_metrics.sql`: Combines metrics about users, clients, covenants, leads, and commissions to track monthly business growth. Includes cumulative totals and first-time purchases to help measure acquisition, engagement, and conversion.

- `mvw_user_funnel_daily_metrics.sql`: Consolidates daily metrics of the user registration and conversion funnel. Tracks valid leads, blocked users, proposal opportunities, and follow-ups created or finalized within 30 days after registration.


- **Documentation:**  
  In the Docs folder, you will find detailed case studies and documentation that explain the business context, data challenges, and the analytical approaches used to derive insights.

## How to Use

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/Thallysbatista/materialized_views.git
   cd your-repository
