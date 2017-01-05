
SELECT COUNT([language]) as Count, language
  FROM tweets.[dbo].[tweets]
  WHERE [language] NOT LIKE 'ENGLISH' AND [language] NOT LIKE 'unreliable' AND [language] NOT LIKE 'Unknown'
  GROUP BY [language]
  ORDER BY Count DESC