-- Pickup zones accounting for 70% of total revenue

WITH zone_rev AS (
  SELECT
    pu.Zone,
    SUM(f.TotalAmount) AS Revenue
  FROM Fact_Trip f
  JOIN Dim_Date d       ON d.DateKey = f.PickupDateKey
  JOIN Dim_Location pu  ON pu.LocationKey = f.PickupLocationKey
  WHERE d.FullDate >= '2025-07-01' AND d.FullDate < '2025-08-01'
  GROUP BY pu.Zone
),
ranked AS (
  SELECT
    Zone,
    Revenue,
    Revenue / SUM(Revenue) OVER () AS Share,
    SUM(Revenue) OVER (
      ORDER BY Revenue DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) / SUM(Revenue) OVER () AS CumShare
  FROM zone_rev
)
SELECT
  Zone,
  Revenue,
  ROUND(100*Share, 2)    AS SharePct,
  ROUND(100*CumShare, 2) AS CumSharePct
FROM ranked
WHERE CumShare <= 0.70
ORDER BY Revenue DESC;
