-- “Airport” trip profile on weekend nights (volume and averages)

SELECT
  d.DayOfWeekName,
  tod.Hour,
  COUNT(*)            AS Trips,
  AVG(f.TripDistance) AS AvgMiles,
  AVG(f.TotalAmount)  AS AvgTotal
FROM Fact_Trip f
JOIN Dim_Date d         ON d.DateKey = f.PickupDateKey
JOIN Dim_TimeOfDay tod  ON tod.TimeKey = f.PickupTimeKey
JOIN Dim_TripCharacteristics tc ON tc.TripCharacteristicsKey = f.TripCharacteristicsKey
WHERE d.FullDate >= '2025-07-01' AND d.FullDate < '2025-08-01'
  AND d.IsWeekend = 1
  AND tod.Hour BETWEEN 20 AND 23
  AND tc.IsAirportTrip = 1
GROUP BY d.DayOfWeekName, tod.Hour
ORDER BY d.DayOfWeekName, tod.Hour;
