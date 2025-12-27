-- Dia + zona de pickup com maior receita

SELECT
  f.PickupDateKey,
  d.FullDate,
  f.PickupLocationKey,
  pu.Zone,
  COUNT(*)           AS Trips,
  SUM(f.TotalAmount) AS Revenue
FROM Fact_Trip f
JOIN Dim_Date d       ON d.DateKey = f.PickupDateKey
JOIN Dim_Location pu  ON pu.LocationKey = f.PickupLocationKey
WHERE d.FullDate >= '2025-07-01' AND d.FullDate < '2025-08-01'
GROUP BY f.PickupDateKey, d.FullDate, f.PickupLocationKey, pu.Zone
ORDER BY Revenue DESC
LIMIT 1;
