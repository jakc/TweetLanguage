select DISTINCT geom.STSrid, COUNT(ID) from [nosde].[dbo].[tweets]
GROUP BY geom.STSrid