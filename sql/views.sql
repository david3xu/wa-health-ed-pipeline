-- ============================================================
-- WA Health ED Pipeline — Synapse SQL Analytics Endpoint Views
-- Run these in: Fabric Lakehouse → SQL analytics endpoint tab
-- ============================================================

-- View 1: Hospitals currently below 4-hour national target (67%)
CREATE OR ALTER VIEW vw_underperforming_hospitals AS
SELECT
    hospital_name,
    health_service,
    time_period_start,
    four_hour_departure_rate,
    wa_average,
    variance_from_wa_avg,
    rolling_4period_avg
FROM gold.ed_waittime_trends
WHERE below_target = 1
ORDER BY time_period_start DESC, four_hour_departure_rate ASC;
GO

-- View 2: WA performance summary for the latest reporting period
CREATE OR ALTER VIEW vw_wa_performance_summary AS
SELECT
    time_period_start,
    COUNT(*) AS hospital_count,
    AVG(four_hour_departure_rate) AS wa_avg_4hr,
    SUM(CASE WHEN below_target = 1 THEN 1 ELSE 0 END) AS hospitals_below_target,
    SUM(CASE WHEN below_target = 0 THEN 1 ELSE 0 END) AS hospitals_on_target,
    ROUND(
        CAST(SUM(CASE WHEN below_target = 1 THEN 1 ELSE 0 END) AS FLOAT) /
        COUNT(*) * 100, 1
    ) AS pct_below_target
FROM gold.ed_waittime_trends
WHERE time_period_start = (
    SELECT MAX(time_period_start) FROM gold.ed_waittime_trends
)
GROUP BY time_period_start;
GO

-- View 3: Health service ranking by average 4-hour rate
CREATE OR ALTER VIEW vw_health_service_ranking AS
SELECT
    health_service,
    COUNT(DISTINCT hospital_name) AS hospital_count,
    ROUND(AVG(four_hour_departure_rate), 2) AS avg_4hr_rate,
    ROUND(MIN(four_hour_departure_rate), 2) AS min_4hr_rate,
    ROUND(MAX(four_hour_departure_rate), 2) AS max_4hr_rate,
    SUM(CASE WHEN below_target = 1 THEN 1 ELSE 0 END) AS periods_below_target
FROM gold.ed_waittime_trends
WHERE health_service IS NOT NULL
GROUP BY health_service
ORDER BY avg_4hr_rate DESC;
GO

-- ============================================================
-- Test queries — run after creating views
-- ============================================================

-- Sanity check: underperforming hospitals latest period
SELECT TOP 10 * FROM vw_underperforming_hospitals;

-- WA summary
SELECT * FROM vw_wa_performance_summary;

-- Best and worst health services
SELECT TOP 5 * FROM vw_health_service_ranking ORDER BY avg_4hr_rate DESC;
SELECT TOP 5 * FROM vw_health_service_ranking ORDER BY avg_4hr_rate ASC;
