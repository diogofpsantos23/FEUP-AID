-- Mudança de receita da 1ª para a 2ª quinzena por borough (diferença e %)

WITH h1 AS (
  SELECT pu.Borough, SUM(f.TotalAmount) AS Revenue
  FROM Fact_Trip f
  JOIN Dim_Date d      ON d.DateKey = f.PickupDateKey
  JOIN Dim_Location pu ON pu.LocationKey = f.PickupLocationKey
  WHERE d.FullDate >= '2025-07-01' AND d.FullDate < '2025-08-01'
    AND d.Day <= 15
  GROUP BY pu.Borough
),
h2 AS (
  SELECT pu.Borough, SUM(f.TotalAmount) AS Revenue
  FROM Fact_Trip f
  JOIN Dim_Date d      ON d.DateKey = f.PickupDateKey
  JOIN Dim_Location pu ON pu.LocationKey = f.PickupLocationKey
  WHERE d.FullDate >= '2025-07-01' AND d.FullDate < '2025-08-01'
    AND d.Day >= 16
  GROUP BY pu.Borough
),
unioned AS (
  SELECT 'H1' AS Half, Borough, Revenue FROM h1
  UNION ALL
  SELECT 'H2' AS Half, Borough, Revenue FROM h2
)
SELECT
  Borough,
  SUM(CASE WHEN Half='H1' THEN Revenue END) AS Rev_H1,
  SUM(CASE WHEN Half='H2' THEN Revenue END) AS Rev_H2,
  (SUM(CASE WHEN Half='H2' THEN Revenue END) - SUM(CASE WHEN Half='H1' THEN Revenue END)) AS Diff_Rev,
  ROUND(
    100 * (SUM(CASE WHEN Half='H2' THEN Revenue END) - SUM(CASE WHEN Half='H1' THEN Revenue END))
    / NULLIF(SUM(CASE WHEN Half='H1' THEN Revenue END), 0),
  2) AS PctChange_H2_vs_H1
FROM unioned
GROUP BY Borough
ORDER BY PctChange_H2_vs_H1 DESC;
