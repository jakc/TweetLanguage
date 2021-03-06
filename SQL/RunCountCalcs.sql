--Script to calculate tweets within postcodes and 
--precalculate normalised values to use in dynamic renderer.

-- Total Tweets per Postcode
UPDATE PC
SET PC.TotTweets = counts.PointCount
FROM [nosde].[dbo].[SA2_2011_3857] PC
JOIN 
(
 SELECT PC.ID, Count(*) as PointCount
 FROM [nosde].[dbo].[tweets] TW 
 JOIN [nosde].[dbo].[SA2_2011_3857] PC
 ON PC.geom.STContains(TW.geom) = 1
 GROUP BY PC.ID
) counts ON PC.ID = counts.ID

-- Total Swears per Postcode
UPDATE PC
SET PC.TotSwears = counts.PointCount
FROM [nosde].[dbo].[SA2_2011_3857] PC
JOIN 
(
 SELECT PC.ID, Count(*) as PointCount
 FROM [nosde].[dbo].[tweets] TW 
 JOIN [nosde].[dbo].[SA2_2011_3857] PC
 ON PC.geom.STContains(TW.geom) = 1 AND TW.swearing = 1
 GROUP BY PC.ID
) counts ON PC.ID = counts.ID

-- Normalised Swears (i.e. TotSwears/TotTweets )
UPDATE PC
SET PC.NormSwears = CAST(PC.TotSwears as float) / CAST(PC.TotTweets as float)
FROM [nosde].[dbo].[SA2_2011_3857] PC

--Toal Arabic per Postcode
UPDATE PC
SET PC.TotArabic = counts.PointCount
FROM [nosde].[dbo].[SA2_2011_3857] PC
JOIN 
(
 SELECT PC.ID, Count(*) as PointCount
 FROM [nosde].[dbo].[tweets] TW 
 JOIN [nosde].[dbo].[SA2_2011_3857] PC
 ON PC.geom.STContains(TW.geom) = 1 AND TW.language = 'Arabic'
 GROUP BY PC.ID
) counts ON PC.ID = counts.ID

-- Normalised Arabic
UPDATE PC
SET PC.NormArabic = CAST(PC.TotArabic as float) / CAST(PC.TotTweets as float)
FROM [nosde].[dbo].[SA2_2011_3857] PC

--Toal Malay per Postcode
UPDATE PC
SET PC.TotMalay = counts.PointCount
FROM [nosde].[dbo].[SA2_2011_3857] PC
JOIN 
(
 SELECT PC.ID, Count(*) as PointCount
 FROM [nosde].[dbo].[tweets] TW 
 JOIN [nosde].[dbo].[SA2_2011_3857] PC
 ON PC.geom.STContains(TW.geom) = 1 AND TW.language = 'Malay'
 GROUP BY PC.ID
) counts ON PC.ID = counts.ID

-- Normalised Malay
UPDATE PC
SET PC.NormMalay = CAST(PC.TotMalay as float) / CAST(PC.TotTweets as float)
FROM [nosde].[dbo].[SA2_2011_3857] PC

--Toal Spanish per Postcode
UPDATE PC
SET PC.TotSpan = counts.PointCount
FROM [nosde].[dbo].[SA2_2011_3857] PC
JOIN 
(
 SELECT PC.ID, Count(*) as PointCount
 FROM [nosde].[dbo].[tweets] TW 
 JOIN [nosde].[dbo].[SA2_2011_3857] PC
 ON PC.geom.STContains(TW.geom) = 1 AND TW.language = 'Spanish'
 GROUP BY PC.ID
) counts ON PC.ID = counts.ID

-- Normalised Spanish
UPDATE PC
SET PC.NormSpan = CAST(PC.TotSpan as float) / CAST(PC.TotTweets as float)
FROM [nosde].[dbo].[SA2_2011_3857] PC

--Toal Indonesian per Postcode
UPDATE PC
SET PC.TotIndo = counts.PointCount
FROM [nosde].[dbo].[SA2_2011_3857] PC
JOIN 
(
 SELECT PC.ID, Count(*) as PointCount
 FROM [nosde].[dbo].[tweets] TW 
 JOIN [nosde].[dbo].[SA2_2011_3857] PC
 ON PC.geom.STContains(TW.geom) = 1 AND TW.language = 'Indonesian'
 GROUP BY PC.ID
) counts ON PC.ID = counts.ID

-- Normalised Indonesian
UPDATE PC
SET PC.NormIndo = CAST(PC.TotIndo as float) / CAST(PC.TotTweets as float)
FROM [nosde].[dbo].[SA2_2011_3857] PC

--Toal Japanese per Postcode
UPDATE PC
SET PC.TotJap = counts.PointCount
FROM [nosde].[dbo].[SA2_2011_3857] PC
JOIN 
(
 SELECT PC.ID, Count(*) as PointCount
 FROM [nosde].[dbo].[tweets] TW 
 JOIN [nosde].[dbo].[SA2_2011_3857] PC
 ON PC.geom.STContains(TW.geom) = 1 AND TW.language = 'Japanese'
 GROUP BY PC.ID
) counts ON PC.ID = counts.ID

-- Normalised Japanese
UPDATE PC
SET PC.NormJap = CAST(PC.TotJap as float) / CAST(PC.TotTweets as float)
FROM [nosde].[dbo].[SA2_2011_3857] PC