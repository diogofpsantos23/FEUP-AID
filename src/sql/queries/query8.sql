-- Compare Fact_Trip aggregation vs Fact_Daily_ZoneVendor for ALL July cells (expected: CellsWithAnyDifference = 0)

WITH
trip_agg AS (
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
),
dzv_agg AS (
  SELECT
    dzv.DateKey,
    dzv.VendorKey,
    dzv.PickupLocationKey,
    dzv.TripsCount       AS Trips_DailyFact,
    dzv.TotalTotalAmount AS Total_DailyFact
  FROM Fact_Daily_ZoneVendor dzv
  JOIN Dim_Date d ON d.DateKey = dzv.DateKey
  WHERE d.FullDate >= '2025-07-01' AND d.FullDate < '2025-08-01'
),
all_keys AS (
  SELECT DateKey, VendorKey, PickupLocationKey FROM trip_agg
  UNION
  SELECT DateKey, VendorKey, PickupLocationKey FROM dzv_agg
),
cmp AS (
  SELECT
    k.DateKey,
    k.VendorKey,
    k.PickupLocationKey,

    dzv.Trips_DailyFact,
    ta.Trips_FromFactTrip,

    dzv.Total_DailyFact,
    ta.Total_FromFactTrip
  FROM all_keys k
  LEFT JOIN dzv_agg  dzv
    ON dzv.DateKey = k.DateKey
   AND dzv.VendorKey = k.VendorKey
   AND dzv.PickupLocationKey = k.PickupLocationKey
  LEFT JOIN trip_agg ta
    ON ta.DateKey = k.DateKey
   AND ta.VendorKey = k.VendorKey
   AND ta.PickupLocationKey = k.PickupLocationKey
)

SELECT
  COUNT(*) AS ComparedCells,

  SUM(CASE WHEN Trips_DailyFact    IS NULL THEN 1 ELSE 0 END) AS MissingInDailyFact,
  SUM(CASE WHEN Trips_FromFactTrip IS NULL THEN 1 ELSE 0 END) AS MissingInFactTrip,

  SUM(
    CASE
      WHEN COALESCE(Trips_FromFactTrip, 0) <> COALESCE(Trips_DailyFact, 0)
      THEN 1 ELSE 0
    END
  ) AS CellsWithTripDifference,

  SUM(
    CASE
      WHEN ROUND(COALESCE(Total_FromFactTrip, 0), 2) <> ROUND(COALESCE(Total_DailyFact, 0), 2)
      THEN 1 ELSE 0
    END
  ) AS CellsWithTotalDifference,

  SUM(
    CASE
      WHEN Trips_DailyFact IS NULL OR Trips_FromFactTrip IS NULL
        OR COALESCE(Trips_FromFactTrip, 0) <> COALESCE(Trips_DailyFact, 0)
        OR ROUND(COALESCE(Total_FromFactTrip, 0), 2) <> ROUND(COALESCE(Total_DailyFact, 0), 2)
      THEN 1 ELSE 0
    END
  ) AS CellsWithAnyDifference

FROM cmp;
