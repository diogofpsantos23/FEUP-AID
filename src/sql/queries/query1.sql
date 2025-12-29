-- Trips and revenue per day in July + monthly total

SELECT
  d.FullDate,
  COUNT(*)           AS Trips,
  SUM(f.TotalAmount) AS Revenue
FROM Fact_Trip f
JOIN Dim_Date d ON d.DateKey = f.PickupDateKey
WHERE d.FullDate >= '2025-07-01' AND d.FullDate < '2025-08-01'
GROUP BY d.FullDate WITH ROLLUP
ORDER BY d.FullDate;
