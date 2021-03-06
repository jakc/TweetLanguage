SELECT PC.ID, Count(*)
FROM [nosde].[dbo].[tweets] TW 
JOIN [nosde].[dbo].[SA2_2011_3857] PC
ON PC.geom.STContains(TW.geom) = 1
GROUP BY PC.ID

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










UPDATE [nosde].[dbo].[SA2_2011_3857]
SET [nosde].[dbo].[SA2_2011_3857].[TotTweets] = counts.pointcount
FROM [nosde].[dbo].[SA2_2011_3857]
JOIN 
(
SELECT PC.SA2_NAME.SA2_NAME, Count(*)
FROM [nosde].[dbo].[tweets] 
JOIN [nosde].[dbo].[SA2_2011_3857]
ON [nosde].[dbo].[SA2_2011_3857].[geom].STContains([nosde].[dbo].[tweets].[geom]) = 1
GROUP BY [nosde].[dbo].[SA2_2011_3857].[ID]
) 
counts ON [nosde].[dbo].[SA2_2011_3857].[ID] = counts.ID
