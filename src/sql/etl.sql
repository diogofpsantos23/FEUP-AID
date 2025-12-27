    CREATE DATABASE IF NOT EXISTS dw;
USE dw;

SET FOREIGN_KEY_CHECKS = 0;

-- (1) Drop (idempotent full reload)
DROP TABLE IF EXISTS Fact_Daily_ZoneVendor;
DROP TABLE IF EXISTS Fact_Trip;

DROP TABLE IF EXISTS Dim_TripCharacteristics;
DROP TABLE IF EXISTS Dim_PassengerGroup;
DROP TABLE IF EXISTS Dim_PaymentType;
DROP TABLE IF EXISTS Dim_RateCode;
DROP TABLE IF EXISTS Dim_Location;
DROP TABLE IF EXISTS Dim_Vendor;
DROP TABLE IF EXISTS Dim_TimeOfDay;
DROP TABLE IF EXISTS Dim_Date;

DROP TABLE IF EXISTS Stg_Trip;

SET FOREIGN_KEY_CHECKS = 1;

-- ============================================================
-- (2) STAGING (processed CSV -> DB staging table)
-- ============================================================

CREATE TABLE Stg_Trip (
  StgTripId BIGINT AUTO_INCREMENT PRIMARY KEY,

  PickupDT  DATETIME NULL,
  DropoffDT DATETIME NULL,

  PickupDateKey  INT NULL,
  DropoffDateKey INT NULL,
  PickupTimeKey  INT NULL,   -- minute-of-day (0..1439)
  DropoffTimeKey INT NULL,

  VendorName VARCHAR(100) NOT NULL,

  PassengerCount INT NULL,
  PassengerGroupCode VARCHAR(10) NOT NULL,

  TripDistance DECIMAL(10,3) NOT NULL,

  RateCodeDesc VARCHAR(50) NOT NULL,
  PaymentTypeDesc VARCHAR(50) NOT NULL,

  StoreAndForwardFlagBool TINYINT(1) NOT NULL,
  IsAirportTrip TINYINT(1) NOT NULL,
  IsCBDTrip TINYINT(1) NOT NULL,
  IsCongestionSurcharge TINYINT(1) NOT NULL,

  FareAmount DECIMAL(10,2) NOT NULL,
  Extra DECIMAL(10,2) NOT NULL,
  MtaTax DECIMAL(10,2) NOT NULL,
  TipAmount DECIMAL(10,2) NOT NULL,
  TollsAmount DECIMAL(10,2) NOT NULL,
  ImprovementSurcharge DECIMAL(10,2) NOT NULL,
  TotalAmount DECIMAL(10,2) NOT NULL,
  CongestionSurcharge DECIMAL(10,2) NOT NULL,
  AirportFee DECIMAL(10,2) NOT NULL,
  CbdCongestionFee DECIMAL(10,2) NOT NULL,

  PUBorough VARCHAR(60) NOT NULL,
  PUZone VARCHAR(120) NOT NULL,
  PUServiceZone VARCHAR(60) NOT NULL,

  DOBorough VARCHAR(60) NOT NULL,
  DOZone VARCHAR(120) NOT NULL,
  DOServiceZone VARCHAR(60) NOT NULL,

  TripDurationMinutes DECIMAL(10,2) NULL,
  AverageSpeedMph DECIMAL(10,3) NULL,
  TotalSurcharges DECIMAL(10,2) NULL,
  NetAmountExclTips DECIMAL(10,2) NULL
);

-- Load the processed CSV directly into Stg_Trip
LOAD DATA LOCAL INFILE '/data/yellow_tripdata_2025-07_26cfceb7.csv'
INTO TABLE Stg_Trip
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@VendorID, @pickup, @dropoff, @passenger_count, @trip_distance, @ratecode, @store_flag, @payment_type,
 @fare, @extra, @mta, @tip, @tolls, @impr, @total, @cong, @airport_fee, @cbd_fee,
 @PU_Borough, @PU_Zone, @PU_Service_zone, @DO_Borough, @DO_Zone, @DO_Service_zone)
SET
  VendorName = TRIM(COALESCE(NULLIF(REPLACE(@VendorID, '\r',''),''),'Unknown')),

  PickupDT  = (@pdt := STR_TO_DATE(REPLACE(@pickup, '\r',''), '%Y-%m-%d %H:%i:%s')),
  DropoffDT = (@ddt := STR_TO_DATE(REPLACE(@dropoff,'\r',''), '%Y-%m-%d %H:%i:%s')),

  PickupDateKey  = CASE WHEN @pdt IS NULL THEN NULL ELSE CAST(DATE_FORMAT(DATE(@pdt), '%Y%m%d') AS UNSIGNED) END,
  DropoffDateKey = CASE WHEN @ddt IS NULL THEN NULL ELSE CAST(DATE_FORMAT(DATE(@ddt), '%Y%m%d') AS UNSIGNED) END,

  PickupTimeKey  = CASE WHEN @pdt IS NULL THEN NULL ELSE (HOUR(@pdt)*60 + MINUTE(@pdt)) END,
  DropoffTimeKey = CASE WHEN @ddt IS NULL THEN NULL ELSE (HOUR(@ddt)*60 + MINUTE(@ddt)) END,

  PassengerCount =
    CASE
      WHEN NULLIF(REPLACE(@passenger_count,'\r',''),'') IS NULL THEN NULL
      ELSE CAST(@passenger_count AS SIGNED)
    END,

  PassengerGroupCode =
    CASE
      WHEN NULLIF(REPLACE(@passenger_count,'\r',''),'') IS NULL THEN 'UNK'
      WHEN CAST(@passenger_count AS SIGNED) = 1 THEN 'P1'
      WHEN CAST(@passenger_count AS SIGNED) = 2 THEN 'P2'
      WHEN CAST(@passenger_count AS SIGNED) IN (3,4) THEN 'P3_4'
      WHEN CAST(@passenger_count AS SIGNED) >= 5 THEN 'P5plus'
      ELSE 'UNK'
    END,

  TripDistance = COALESCE(NULLIF(REPLACE(@trip_distance,'\r',''),''),0) + 0,

  RateCodeDesc    = TRIM(COALESCE(NULLIF(REPLACE(@ratecode,'\r',''),''),'Unknown')),
  PaymentTypeDesc = TRIM(COALESCE(NULLIF(REPLACE(@payment_type,'\r',''),''),'Unknown')),

  StoreAndForwardFlagBool =
    CASE
      WHEN UPPER(TRIM(COALESCE(NULLIF(REPLACE(@store_flag,'\r',''),''),'N'))) IN ('Y','YES','TRUE','1') THEN 1
      ELSE 0
    END,

  FareAmount  = COALESCE(NULLIF(REPLACE(@fare,'\r',''),''),0) + 0,
  Extra       = COALESCE(NULLIF(REPLACE(@extra,'\r',''),''),0) + 0,
  MtaTax      = COALESCE(NULLIF(REPLACE(@mta,'\r',''),''),0) + 0,
  TipAmount   = COALESCE(NULLIF(REPLACE(@tip,'\r',''),''),0) + 0,
  TollsAmount = COALESCE(NULLIF(REPLACE(@tolls,'\r',''),''),0) + 0,
  ImprovementSurcharge = COALESCE(NULLIF(REPLACE(@impr,'\r',''),''),0) + 0,
  TotalAmount = COALESCE(NULLIF(REPLACE(@total,'\r',''),''),0) + 0,
  CongestionSurcharge = COALESCE(NULLIF(REPLACE(@cong,'\r',''),''),0) + 0,
  AirportFee  = COALESCE(NULLIF(REPLACE(@airport_fee,'\r',''),''),0) + 0,
  CbdCongestionFee = COALESCE(NULLIF(REPLACE(@cbd_fee,'\r',''),''),0) + 0,

  PUBorough = TRIM(COALESCE(NULLIF(REPLACE(@PU_Borough,'\r',''),''),'Unknown')),
  PUZone = TRIM(COALESCE(NULLIF(REPLACE(@PU_Zone,'\r',''),''),'Unknown')),
  PUServiceZone = TRIM(COALESCE(NULLIF(REPLACE(@PU_Service_zone,'\r',''),''),'Unknown')),

  DOBorough = TRIM(COALESCE(NULLIF(REPLACE(@DO_Borough,'\r',''),''),'Unknown')),
  DOZone = TRIM(COALESCE(NULLIF(REPLACE(@DO_Zone,'\r',''),''),'Unknown')),
  DOServiceZone = TRIM(COALESCE(NULLIF(REPLACE(@DO_Service_zone,'\r',''),''),'Unknown')),

  IsAirportTrip =
    CASE
      WHEN (COALESCE(NULLIF(REPLACE(@airport_fee,'\r',''),''),0)+0) > 0
        OR TRIM(COALESCE(NULLIF(REPLACE(@PU_Service_zone,'\r',''),''),'')) = 'Airports'
        OR TRIM(COALESCE(NULLIF(REPLACE(@PU_Zone,'\r',''),''),'')) LIKE '%Airport%'
      THEN 1 ELSE 0
    END,

  IsCBDTrip = CASE WHEN (COALESCE(NULLIF(REPLACE(@cbd_fee,'\r',''),''),0)+0) > 0 THEN 1 ELSE 0 END,
  IsCongestionSurcharge = CASE WHEN (COALESCE(NULLIF(REPLACE(@cong,'\r',''),''),0)+0) > 0 THEN 1 ELSE 0 END,

  TripDurationMinutes =
    CASE
      WHEN @pdt IS NULL OR @ddt IS NULL THEN NULL
      ELSE ROUND(TIMESTAMPDIFF(SECOND, @pdt, @ddt)/60.0, 2)
    END,

  AverageSpeedMph =
    CASE
      WHEN @pdt IS NULL OR @ddt IS NULL THEN NULL
      WHEN TIMESTAMPDIFF(SECOND, @pdt, @ddt) <= 0 THEN NULL
      ELSE ROUND( (COALESCE(NULLIF(REPLACE(@trip_distance,'\r',''),''),0)+0) / (TIMESTAMPDIFF(SECOND, @pdt, @ddt)/3600.0), 3)
    END,

  TotalSurcharges =
    ROUND(
      (COALESCE(NULLIF(REPLACE(@extra,'\r',''),''),0)+0) +
      (COALESCE(NULLIF(REPLACE(@mta,'\r',''),''),0)+0) +
      (COALESCE(NULLIF(REPLACE(@tolls,'\r',''),''),0)+0) +
      (COALESCE(NULLIF(REPLACE(@impr,'\r',''),''),0)+0) +
      (COALESCE(NULLIF(REPLACE(@cong,'\r',''),''),0)+0) +
      (COALESCE(NULLIF(REPLACE(@airport_fee,'\r',''),''),0)+0) +
      (COALESCE(NULLIF(REPLACE(@cbd_fee,'\r',''),''),0)+0)
    , 2),

  NetAmountExclTips = ROUND((COALESCE(NULLIF(REPLACE(@total,'\r',''),''),0)+0) - (COALESCE(NULLIF(REPLACE(@tip,'\r',''),''),0)+0), 2)
;

-- Basic validity gate
DELETE FROM Stg_Trip
WHERE PickupDT IS NULL OR DropoffDT IS NULL OR PickupDT >= DropoffDT;

SET @min_date := (SELECT MIN(DATE(PickupDT)) FROM Stg_Trip);
SET @max_date := (SELECT MAX(DATE(DropoffDT)) FROM Stg_Trip);

-- ============================================================
-- (4) DIMENSIONS
-- ============================================================

CREATE TABLE Dim_Date (
  DateKey INT PRIMARY KEY,
  FullDate DATE NOT NULL,
  Day TINYINT NOT NULL,
  Month TINYINT NOT NULL,
  MonthName VARCHAR(15) NOT NULL,
  Quarter TINYINT NOT NULL,
  Year SMALLINT NOT NULL,
  DayOfWeekNumber TINYINT NOT NULL,
  DayOfWeekName VARCHAR(10) NOT NULL,
  IsWeekend TINYINT(1) NOT NULL
);

INSERT INTO Dim_Date
  (DateKey, FullDate, Day, Month, MonthName, Quarter, Year, DayOfWeekNumber, DayOfWeekName, IsWeekend)
SELECT
  CAST(DATE_FORMAT(d, '%Y%m%d') AS UNSIGNED) AS DateKey,
  d AS FullDate,
  DAY(d) AS Day,
  MONTH(d) AS Month,
  DATE_FORMAT(d, '%M') AS MonthName,
  QUARTER(d) AS Quarter,
  YEAR(d) AS Year,
  ((DAYOFWEEK(d)+5) % 7) + 1 AS DayOfWeekNumber,
  DATE_FORMAT(d, '%W') AS DayOfWeekName,
  CASE WHEN DAYOFWEEK(d) IN (1,7) THEN 1 ELSE 0 END AS IsWeekend
FROM (
  SELECT DATE_ADD(@min_date, INTERVAL n DAY) AS d
  FROM (
    SELECT (a.n + 10*b.n + 100*c.n + 1000*d.n) AS n
    FROM
      (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
       UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a
    CROSS JOIN
      (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
       UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b
    CROSS JOIN
      (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
       UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) c
    CROSS JOIN
      (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
       UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d
  ) nums
  WHERE DATE_ADD(@min_date, INTERVAL nums.n DAY) <= @max_date
) x;

CREATE TABLE Dim_TimeOfDay (
  TimeKey INT PRIMARY KEY,   -- minute-of-day (0..1439)
  Hour TINYINT NOT NULL,
  Minute TINYINT NOT NULL,
  TimeLabel VARCHAR(20) NOT NULL,
  TimeBucket VARCHAR(20) NOT NULL
);

INSERT INTO Dim_TimeOfDay(TimeKey, Hour, Minute, TimeLabel, TimeBucket)
SELECT
  (h.hr*60 + m.mn) AS TimeKey,
  h.hr AS Hour,
  m.mn AS Minute,
  CONCAT(LPAD(h.hr,2,'0'),':',LPAD(m.mn,2,'0')) AS TimeLabel,
  CASE
    WHEN h.hr BETWEEN 0 AND 4 THEN 'LateNight'
    WHEN h.hr BETWEEN 5 AND 11 THEN 'Morning'
    WHEN h.hr BETWEEN 12 AND 16 THEN 'Afternoon'
    WHEN h.hr BETWEEN 17 AND 20 THEN 'Evening'
    ELSE 'Night'
  END AS TimeBucket
FROM
  (SELECT 0 hr UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
   UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11
   UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15 UNION ALL SELECT 16 UNION ALL SELECT 17
   UNION ALL SELECT 18 UNION ALL SELECT 19 UNION ALL SELECT 20 UNION ALL SELECT 21 UNION ALL SELECT 22 UNION ALL SELECT 23) h
CROSS JOIN
  (SELECT 0 mn UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
   UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11
   UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15 UNION ALL SELECT 16 UNION ALL SELECT 17
   UNION ALL SELECT 18 UNION ALL SELECT 19 UNION ALL SELECT 20 UNION ALL SELECT 21 UNION ALL SELECT 22 UNION ALL SELECT 23
   UNION ALL SELECT 24 UNION ALL SELECT 25 UNION ALL SELECT 26 UNION ALL SELECT 27 UNION ALL SELECT 28 UNION ALL SELECT 29
   UNION ALL SELECT 30 UNION ALL SELECT 31 UNION ALL SELECT 32 UNION ALL SELECT 33 UNION ALL SELECT 34 UNION ALL SELECT 35
   UNION ALL SELECT 36 UNION ALL SELECT 37 UNION ALL SELECT 38 UNION ALL SELECT 39 UNION ALL SELECT 40 UNION ALL SELECT 41
   UNION ALL SELECT 42 UNION ALL SELECT 43 UNION ALL SELECT 44 UNION ALL SELECT 45 UNION ALL SELECT 46 UNION ALL SELECT 47
   UNION ALL SELECT 48 UNION ALL SELECT 49 UNION ALL SELECT 50 UNION ALL SELECT 51 UNION ALL SELECT 52 UNION ALL SELECT 53
   UNION ALL SELECT 54 UNION ALL SELECT 55 UNION ALL SELECT 56 UNION ALL SELECT 57 UNION ALL SELECT 58 UNION ALL SELECT 59) m;

CREATE TABLE Dim_Vendor (
  VendorKey INT AUTO_INCREMENT PRIMARY KEY,
  VendorCode INT NULL,
  VendorName VARCHAR(100) NOT NULL,
  UNIQUE (VendorCode),
  UNIQUE (VendorName)
);

INSERT INTO Dim_Vendor(VendorCode, VendorName)
SELECT DISTINCT
  CASE
    WHEN VendorName = 'Curb Mobility, LLC' THEN 1
    WHEN VendorName = 'Creative Mobile Technologies, LLC' THEN 2
    WHEN VendorName = 'Helix' THEN 3
    ELSE NULL
  END AS VendorCode,
  VendorName
FROM Stg_Trip;

CREATE TABLE Dim_Location (
  LocationKey INT AUTO_INCREMENT PRIMARY KEY,
  LocationID INT UNSIGNED NOT NULL,
  Borough VARCHAR(60) NOT NULL,
  Zone VARCHAR(120) NOT NULL,
  ServiceZone VARCHAR(60) NOT NULL,
  IsAirport TINYINT(1) NOT NULL,
  IsCBD TINYINT(1) NOT NULL,
  UNIQUE (LocationID),
  UNIQUE (Borough, Zone, ServiceZone)
);

INSERT INTO Dim_Location(LocationID, Borough, Zone, ServiceZone, IsAirport, IsCBD)
SELECT
  CAST(CRC32(CONCAT(Borough,'|',Zone,'|',ServiceZone)) AS UNSIGNED) AS LocationID,
  Borough, Zone, ServiceZone,
  MAX(IsAirport) AS IsAirport,
  MAX(IsCBD) AS IsCBD
FROM (
  SELECT
    PUBorough AS Borough, PUZone AS Zone, PUServiceZone AS ServiceZone,
    CASE WHEN AirportFee > 0 OR PUServiceZone='Airports' OR PUZone LIKE '%Airport%' THEN 1 ELSE 0 END AS IsAirport,
    CASE WHEN CbdCongestionFee > 0 THEN 1 ELSE 0 END AS IsCBD
  FROM Stg_Trip
  UNION ALL
  SELECT
    DOBorough, DOZone, DOServiceZone,
    CASE WHEN AirportFee > 0 OR DOServiceZone='Airports' OR DOZone LIKE '%Airport%' THEN 1 ELSE 0 END,
    CASE WHEN CbdCongestionFee > 0 THEN 1 ELSE 0 END
  FROM Stg_Trip
) x
GROUP BY Borough, Zone, ServiceZone;

CREATE TABLE Dim_RateCode (
  RateCodeKey INT AUTO_INCREMENT PRIMARY KEY,
  RateCodeDesc VARCHAR(50) NOT NULL,
  UNIQUE (RateCodeDesc)
);

INSERT INTO Dim_RateCode(RateCodeDesc)
SELECT DISTINCT RateCodeDesc
FROM Stg_Trip;

CREATE TABLE Dim_PaymentType (
  PaymentTypeKey INT AUTO_INCREMENT PRIMARY KEY,
  PaymentTypeDesc VARCHAR(60) NOT NULL,
  UNIQUE (PaymentTypeDesc)
);

INSERT INTO Dim_PaymentType(PaymentTypeDesc)
SELECT DISTINCT PaymentTypeDesc
FROM Stg_Trip;

CREATE TABLE Dim_PassengerGroup (
  PassengerGroupKey INT AUTO_INCREMENT PRIMARY KEY,
  PassengerGroupCode VARCHAR(10) NOT NULL,
  PassengerGroupDesc VARCHAR(50) NOT NULL,
  MinPassengers TINYINT NOT NULL,
  MaxPassengers TINYINT NULL,
  UNIQUE (PassengerGroupCode)
);

INSERT INTO Dim_PassengerGroup(PassengerGroupCode, PassengerGroupDesc, MinPassengers, MaxPassengers)
VALUES
('P1','Solo',1,1),
('P2','Couple',2,2),
('P3_4','Small group (3-4)',3,4),
('P5plus','Large group (5+)',5,NULL),
('UNK','Unknown',0,NULL);

CREATE TABLE Dim_TripCharacteristics (
  TripCharacteristicsKey INT AUTO_INCREMENT PRIMARY KEY,
  StoreAndForwardFlagBool TINYINT(1) NOT NULL,
  IsAirportTrip TINYINT(1) NOT NULL,
  IsCBDTrip TINYINT(1) NOT NULL,
  IsCongestionSurcharge TINYINT(1) NOT NULL,
  UNIQUE (StoreAndForwardFlagBool, IsAirportTrip, IsCBDTrip, IsCongestionSurcharge)
);

INSERT INTO Dim_TripCharacteristics(StoreAndForwardFlagBool, IsAirportTrip, IsCBDTrip, IsCongestionSurcharge)
SELECT DISTINCT
  StoreAndForwardFlagBool, IsAirportTrip, IsCBDTrip, IsCongestionSurcharge
FROM Stg_Trip;

-- ============================================================
-- (5) FACTS
-- ============================================================

CREATE TABLE Fact_Trip (
  FactTripKey BIGINT AUTO_INCREMENT PRIMARY KEY,

  PickupDateKey  INT NOT NULL,
  DropoffDateKey INT NOT NULL,
  PickupTimeKey  INT NOT NULL,
  DropoffTimeKey INT NOT NULL,

  VendorKey INT NOT NULL,
  PickupLocationKey INT NOT NULL,
  DropoffLocationKey INT NOT NULL,
  RateCodeKey INT NOT NULL,
  PaymentTypeKey INT NOT NULL,
  TripCharacteristicsKey INT NOT NULL,
  PassengerGroupKey INT NOT NULL,

  TripDistance DECIMAL(10,3) NOT NULL,
  FareAmount DECIMAL(10,2) NOT NULL,
  Extra DECIMAL(10,2) NOT NULL,
  MtaTax DECIMAL(10,2) NOT NULL,
  TipAmount DECIMAL(10,2) NOT NULL,
  TollsAmount DECIMAL(10,2) NOT NULL,
  ImprovementSurcharge DECIMAL(10,2) NOT NULL,
  TotalAmount DECIMAL(10,2) NOT NULL,
  CongestionSurcharge DECIMAL(10,2) NOT NULL,
  AirportFee DECIMAL(10,2) NOT NULL,
  CbdCongestionFee DECIMAL(10,2) NOT NULL,
  TripDurationMinutes DECIMAL(10,2) NOT NULL,
  AverageSpeedMph DECIMAL(10,3) NULL,
  TotalSurcharges DECIMAL(10,2) NOT NULL,
  NetAmountExclTips DECIMAL(10,2) NOT NULL
);

INSERT INTO Fact_Trip (
  PickupDateKey, DropoffDateKey, PickupTimeKey, DropoffTimeKey,
  VendorKey, PickupLocationKey, DropoffLocationKey,
  RateCodeKey, PaymentTypeKey, TripCharacteristicsKey, PassengerGroupKey,
  TripDistance, FareAmount, Extra, MtaTax, TipAmount, TollsAmount, ImprovementSurcharge,
  TotalAmount, CongestionSurcharge, AirportFee, CbdCongestionFee,
  TripDurationMinutes, AverageSpeedMph, TotalSurcharges, NetAmountExclTips
)
SELECT
  s.PickupDateKey, s.DropoffDateKey, s.PickupTimeKey, s.DropoffTimeKey,
  v.VendorKey,
  pu.LocationKey,
  dloc.LocationKey,
  rc.RateCodeKey,
  pt.PaymentTypeKey,
  tc.TripCharacteristicsKey,
  pg.PassengerGroupKey,
  s.TripDistance, s.FareAmount, s.Extra, s.MtaTax, s.TipAmount, s.TollsAmount, s.ImprovementSurcharge,
  s.TotalAmount, s.CongestionSurcharge, s.AirportFee, s.CbdCongestionFee,
  s.TripDurationMinutes, s.AverageSpeedMph, s.TotalSurcharges, s.NetAmountExclTips
FROM Stg_Trip s
JOIN Dim_Vendor v ON v.VendorName = s.VendorName
JOIN Dim_Location pu ON pu.Borough=s.PUBorough AND pu.Zone=s.PUZone AND pu.ServiceZone=s.PUServiceZone
JOIN Dim_Location dloc ON dloc.Borough=s.DOBorough AND dloc.Zone=s.DOZone AND dloc.ServiceZone=s.DOServiceZone
JOIN Dim_RateCode rc ON rc.RateCodeDesc = s.RateCodeDesc
JOIN Dim_PaymentType pt ON pt.PaymentTypeDesc = s.PaymentTypeDesc
JOIN Dim_TripCharacteristics tc
  ON tc.StoreAndForwardFlagBool = s.StoreAndForwardFlagBool
 AND tc.IsAirportTrip = s.IsAirportTrip
 AND tc.IsCBDTrip = s.IsCBDTrip
 AND tc.IsCongestionSurcharge = s.IsCongestionSurcharge
JOIN Dim_PassengerGroup pg ON pg.PassengerGroupCode = s.PassengerGroupCode;

ALTER TABLE Fact_Trip
  ADD CONSTRAINT fk_facttrip_pickupdate FOREIGN KEY (PickupDateKey) REFERENCES Dim_Date(DateKey),
  ADD CONSTRAINT fk_facttrip_dropoffdate FOREIGN KEY (DropoffDateKey) REFERENCES Dim_Date(DateKey),
  ADD CONSTRAINT fk_facttrip_pickuptime FOREIGN KEY (PickupTimeKey) REFERENCES Dim_TimeOfDay(TimeKey),
  ADD CONSTRAINT fk_facttrip_dropofftime FOREIGN KEY (DropoffTimeKey) REFERENCES Dim_TimeOfDay(TimeKey),
  ADD CONSTRAINT fk_facttrip_vendor FOREIGN KEY (VendorKey) REFERENCES Dim_Vendor(VendorKey),
  ADD CONSTRAINT fk_facttrip_puloc FOREIGN KEY (PickupLocationKey) REFERENCES Dim_Location(LocationKey),
  ADD CONSTRAINT fk_facttrip_doloc FOREIGN KEY (DropoffLocationKey) REFERENCES Dim_Location(LocationKey),
  ADD CONSTRAINT fk_facttrip_rate FOREIGN KEY (RateCodeKey) REFERENCES Dim_RateCode(RateCodeKey),
  ADD CONSTRAINT fk_facttrip_pay FOREIGN KEY (PaymentTypeKey) REFERENCES Dim_PaymentType(PaymentTypeKey),
  ADD CONSTRAINT fk_facttrip_tc FOREIGN KEY (TripCharacteristicsKey) REFERENCES Dim_TripCharacteristics(TripCharacteristicsKey),
  ADD CONSTRAINT fk_facttrip_pg FOREIGN KEY (PassengerGroupKey) REFERENCES Dim_PassengerGroup(PassengerGroupKey);

CREATE TABLE Fact_Daily_ZoneVendor (
  DateKey INT NOT NULL,
  VendorKey INT NOT NULL,
  PickupLocationKey INT NOT NULL,

  TripsCount INT NOT NULL,
  TotalTripDistance DECIMAL(14,3) NOT NULL,
  TotalFareAmount DECIMAL(14,2) NOT NULL,
  TotalTotalAmount DECIMAL(14,2) NOT NULL,
  TotalTipAmount DECIMAL(14,2) NOT NULL,
  TotalTollsAmount DECIMAL(14,2) NOT NULL,
  TotalDurationMinutes DECIMAL(14,2) NOT NULL,

  MaxSimultaneousTrips INT NOT NULL,
  OpenTripsAtPeakHourMorning INT NOT NULL, -- 09:00 (540)
  OpenTripsAtPeakHourNight INT NOT NULL,   -- 18:00 (1080)

  PRIMARY KEY (DateKey, VendorKey, PickupLocationKey),
  CONSTRAINT fk_factdaily_date FOREIGN KEY (DateKey) REFERENCES Dim_Date(DateKey),
  CONSTRAINT fk_factdaily_vendor FOREIGN KEY (VendorKey) REFERENCES Dim_Vendor(VendorKey),
  CONSTRAINT fk_factdaily_loc FOREIGN KEY (PickupLocationKey) REFERENCES Dim_Location(LocationKey)
);

INSERT INTO Fact_Daily_ZoneVendor
  (DateKey, VendorKey, PickupLocationKey,
   TripsCount, TotalTripDistance, TotalFareAmount, TotalTotalAmount,
   TotalTipAmount, TotalTollsAmount, TotalDurationMinutes,
   MaxSimultaneousTrips, OpenTripsAtPeakHourMorning, OpenTripsAtPeakHourNight)
SELECT
  b.DateKey, b.VendorKey, b.PickupLocationKey,
  b.TripsCount, b.TotalTripDistance, b.TotalFareAmount, b.TotalTotalAmount,
  b.TotalTipAmount, b.TotalTollsAmount, b.TotalDurationMinutes,
  COALESCE(m.MaxSimultaneousTrips,0) AS MaxSimultaneousTrips,
  COALESCE(o.OpenTripsAt09,0)        AS OpenTripsAtPeakHourMorning,
  COALESCE(o.OpenTripsAt18,0)        AS OpenTripsAtPeakHourNight
FROM
  (
    SELECT
      f.PickupDateKey AS DateKey,
      f.VendorKey,
      f.PickupLocationKey,
      COUNT(*) AS TripsCount,
      SUM(f.TripDistance) AS TotalTripDistance,
      SUM(f.FareAmount) AS TotalFareAmount,
      SUM(f.TotalAmount) AS TotalTotalAmount,
      SUM(f.TipAmount) AS TotalTipAmount,
      SUM(f.TollsAmount) AS TotalTollsAmount,
      SUM(f.TripDurationMinutes) AS TotalDurationMinutes
    FROM Fact_Trip f
    GROUP BY f.PickupDateKey, f.VendorKey, f.PickupLocationKey
  ) b
LEFT JOIN
  (
    SELECT
      f.PickupDateKey AS DateKey,
      f.VendorKey,
      f.PickupLocationKey,
      SUM(CASE
            WHEN f.PickupTimeKey <= 540
             AND f.DropoffDateKey = f.PickupDateKey
             AND f.DropoffTimeKey >= 540 THEN 1 ELSE 0 END) AS OpenTripsAt09,
      SUM(CASE
            WHEN f.PickupTimeKey <= 1080
             AND f.DropoffDateKey = f.PickupDateKey
             AND f.DropoffTimeKey >= 1080 THEN 1 ELSE 0 END) AS OpenTripsAt18
    FROM Fact_Trip f
    GROUP BY f.PickupDateKey, f.VendorKey, f.PickupLocationKey
  ) o
  ON o.DateKey=b.DateKey AND o.VendorKey=b.VendorKey AND o.PickupLocationKey=b.PickupLocationKey
LEFT JOIN
  (
    SELECT
      DateKey, VendorKey, PickupLocationKey,
      COALESCE(MAX(concurrent_trips),0) AS MaxSimultaneousTrips
    FROM (
      SELECT
        DateKey, VendorKey, PickupLocationKey,
        SUM(delta) OVER (
          PARTITION BY DateKey, VendorKey, PickupLocationKey
          ORDER BY t, ord
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS concurrent_trips
      FROM (
        SELECT PickupDateKey AS DateKey, VendorKey, PickupLocationKey,
               PickupTimeKey AS t,  1 AS delta, 1 AS ord
        FROM Fact_Trip
        UNION ALL
        SELECT PickupDateKey, VendorKey, PickupLocationKey,
               DropoffTimeKey AS t, -1 AS delta, 0 AS ord
        FROM Fact_Trip
        WHERE DropoffDateKey = PickupDateKey
      ) events
    ) running
    GROUP BY DateKey, VendorKey, PickupLocationKey
  ) m
  ON m.DateKey=b.DateKey AND m.VendorKey=b.VendorKey AND m.PickupLocationKey=b.PickupLocationKey;

-- Sanity checks
-- SELECT 'Stg_Trip' tbl, COUNT(*) cnt FROM Stg_Trip
-- UNION ALL SELECT 'Fact_Trip', COUNT(*) FROM Fact_Trip
-- UNION ALL SELECT 'Fact_Daily_ZoneVendor', COUNT(*) FROM Fact_Daily_ZoneVendor;
