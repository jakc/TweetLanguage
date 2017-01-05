SELECT PC.ID, Count(TW.ID)
FROM [nosde].[dbo].[SA2_2011_WGS84] PC
JOIN [nosde].[dbo].[tweets] TW
ON PC.geom.STIntersects(TW.geom) = 1
GROUP BY PC.ID
