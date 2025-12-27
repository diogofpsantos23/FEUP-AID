-- Comparação de métricas entre Fact_Trip e Fact_Daily_ZoneVendor (diferenças)

WITH trip_agg AS (
  SELECT
    f.PickupDateKey     AS DateKey,
    f.VendorKey         AS VendorKey,
    f.PickupLocationKey AS PickupLocationKey,
    COUNT(*)            AS Trips_FromFactTrip,
    SUM(f.TotalAmount)  AS Total_FromFactTrip
  FROM Fact_Trip f
  JOIN Dim_Date d ON d.DateKey = f.PickupDateKey
  WHERE d.FullDate >= '2025-07-01' AND d.FullDate < '2025-08-01'
  GROUP BY f.PickupDateKey, f.VendorKey, f.PickupLocationKey
)
SELECT
  d.FullDate,
  v.VendorName,
  pu.Zone,
  dzv.TripsCount        AS Trips_DailyFact,
  ta.Trips_FromFactTrip,
  (ta.Trips_FromFactTrip - dzv.TripsCount) AS Diff_Trips,
  dzv.TotalTotalAmount  AS Total_DailyFact,
  ta.Total_FromFactTrip,
  (ta.Total_FromFactTrip - dzv.TotalTotalAmount) AS Diff_Total
FROM Fact_Daily_ZoneVendor dzv
JOIN trip_agg ta
  ON ta.DateKey = dzv.DateKey
 AND ta.VendorKey = dzv.VendorKey
 AND ta.PickupLocationKey = dzv.PickupLocationKey
JOIN Dim_Date d      ON d.DateKey = dzv.DateKey
JOIN Dim_Vendor v    ON v.VendorKey = dzv.VendorKey
JOIN Dim_Location pu ON pu.LocationKey = dzv.PickupLocationKey
WHERE d.FullDate >= '2025-07-01' AND d.FullDate < '2025-08-01'
ORDER BY ABS(Diff_Trips) DESC, ABS(Diff_Total) DESC
LIMIT 50;
