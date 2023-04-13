/* Indicators */

SELECT
    COUNT(*)
FROM
    bindings.`binding01`

SELECT
    SUM(volume)
FROM
    bindings.`binding01`

SELECT
    COUNT(DISTINCT(flags))
FROM
    bindings.`binding01`
    
SELECT
    COUNT(DISTINCT(base_symbol))
FROM
    bindings.`binding01`

/* Structure of the data */

SELECT
    base_symbol,
    COUNT(*) as counts
FROM
    bindings.`binding01`
GROUP BY base_symbol
ORDER BY counts DESC
LIMIT 20


SELECT
   type,
   SUM(volume) as total
FROM
   bindings.`binding01`
GROUP BY type


SELECT
    base_symbol,
    (SUM(ask) / SUM(bid) - 1) * 100 as ratio
FROM
    bindings.`binding01`
GROUP BY base_symbol
HAVING SUM(bid) > 0
ORDER BY ratio DESC
LIMIT 10

/* Market Cap data */

SELECT
   Industry,
   SUM(`Market Cap`) as total_cap
FROM
   bindings.`binding03`
GROUP BY Industry
ORDER BY total_cap DESC

SELECT
    Country,
    Industry,
    SUM(`Market Cap`) as cap
FROM
    bindings.`binding03` 
GROUP BY Country, Industry
ORDER BY cap DESC

/* Join tables */

SELECT
    mt.Industry,
    SUM(os.volume) as total
FROM
    bindings.`binding01` AS os
INNER JOIN bindings.`binding03` AS mt 
    ON os.base_symbol = mt.Ticker
GROUP BY mt.Industry
ORDER BY total DESC