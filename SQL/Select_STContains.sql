SELECT PC.SA2_NAME, Count(*)
FROM [nosde].[dbo].[tweets] TW 
JOIN [nosde].[dbo].[SA2_2011_3857] PC
ON PC.geom.STContains(TW.geom) = 1
GROUP BY PC.SA2_NAME


UPDATE [nosde].[dbo].[SA2_2011_3857] PC
SET PC.TotTweets
