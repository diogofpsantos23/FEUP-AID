-- Payment distribution (cash vs card) by pickup borough

SELECT
  pu.Borough,
  SUM(pt.PaymentTypeDesc = 'Cash')        AS Trips_Cash,
  SUM(pt.PaymentTypeDesc = 'Credit card') AS Trips_Card,
  COUNT(*) AS Trips_Total
FROM Fact_Trip f
JOIN Dim_Date d          ON d.DateKey = f.PickupDateKey
JOIN Dim_Location pu     ON pu.LocationKey = f.PickupLocationKey
JOIN Dim_PaymentType pt  ON pt.PaymentTypeKey = f.PaymentTypeKey
WHERE d.FullDate >= '2025-07-01' AND d.FullDate < '2025-08-01'
GROUP BY pu.Borough
ORDER BY Trips_Total DESC;
