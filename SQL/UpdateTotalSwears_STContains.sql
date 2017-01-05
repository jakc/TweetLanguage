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







