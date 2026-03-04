#  Wind Turbine SCADA Data Analysis (PostgreSQL)

##  Project Overview
This project focuses on the end-to-end analysis of Wind Turbine SCADA (Supervisory Control and Data Acquisition) data using **PostgreSQL**. The goal is to transform raw sensor data into actionable insights regarding energy production, turbine performance, and meteorological impacts.


Database Schema
The project uses a normalized structure:
`wind_scada_data`: Main fact table containing 10-minute interval sensor logs.
 `turbine_info`: Dimension table with metadata (Model, Location, Coordinates).
`v_performance_summary`: A calculated View for Power BI integration.

 Key Insights Extracted
1.  Energy Production (MWh):Calculated monthly energy yield by converting 10-minute kW samples into Megawatt-hours.
2.  Performance Gap: Analyzed the deviation between **Actual Power and the Theoretical Power Curve to identify underperformance.
3.  Meteorological Analysis: Identified extreme wind events (>20 m/s) and "Dead Days" (Avg wind <3 m/s).
4.  **Wind Dynamics: Used the `LAG()` function to calculate instantaneous wind speed changes, helping detect sudden gusts that could stress turbine components.

SQL Highlights
Advanced Performance Ranking
```sql
WITH ranked_data AS (
    SELECT date_time_utc, active_power_kw, 
           RANK() OVER (PARTITION BY DATE_TRUNC('day', date_time_utc) 
           ORDER BY active_power_kw DESC) AS rank
    FROM wind_scada_data
)
SELECT * FROM ranked_data WHERE rank <= 3;
