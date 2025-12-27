-- Horas mais movimentadas (pickups) em Manhattan + ticket médio

SELECT
  tod.Hour,
  COUNT(*)           AS Trips,
  AVG(f.TotalAmount) AS AvgTotal
FROM Fact_Trip f
JOIN Dim_Date d         ON d.DateKey = f.PickupDateKey
JOIN Dim_TimeOfDay tod  ON tod.TimeKey = f.PickupTimeKey
JOIN Dim_Location pu    ON pu.LocationKey = f.PickupLocationKey
WHERE d.FullDate >= '2025-07-01' AND d.FullDate < '2025-08-01'
  AND pu.Borough = 'Manhattan'     -- SLICE
GROUP BY tod.Hour
ORDER BY Trips DESC;               -- SORT
