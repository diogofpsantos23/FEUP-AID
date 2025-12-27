-- Detalhe das 50 viagens mais caras dentro da célula escolhida
-- params: PickupDateKey:int, PickupLocationKey:int

SELECT
  f.FactTripKey,
  f.TripDistance,
  f.TripDurationMinutes,
  f.AverageSpeedMph,
  f.TotalAmount,
  f.TipAmount,
  f.VendorKey,
  f.PaymentTypeKey
FROM Fact_Trip f
WHERE f.PickupDateKey = %s
  AND f.PickupLocationKey = %s
ORDER BY f.TotalAmount DESC
LIMIT 50;
