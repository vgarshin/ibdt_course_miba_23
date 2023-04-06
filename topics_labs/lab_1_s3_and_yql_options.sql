/* useful: https://cloud.yandex.ru/blog/posts/2023/03/yandex-query-and-yandex-datalens?utm_source=telegram&utm_medium=post&utm_campaign=datalens_community (Russian)

   make a connection `options01` with public (!) access for DataLens connection
   basic query example with use of connection `options01` */

SELECT
   *
FROM
   `options01`.`data/L3_options_20161101.parquet/part*`
WITH
   (
       format='parquet',
       SCHEMA=(
           base_symbol string,
           base_price float,
           flags string,
           option_symbol string,
           type string,
           expiration string,
           date string,
           strike float,
           last float,
           bid float,
           ask float,
           volume float,
           open_interest int32,
           t1_open_interest int32,
           iv_mean float,
           iv_bid float,
           iv_ask float,
           delta float,
           gamma float,
           theta float,
           vega float,
           aka string
        )
   )
LIMIT 5;

/* total records (about 5 min runtime) */

SELECT
   COUNT(*)
FROM
   `options01`.`data/L3_options_20*.parquet/part*`
WITH
   (
       format='parquet',
       SCHEMA=(
           base_symbol string,
           base_price float,
           flags string,
           option_symbol string,
           type string,
           expiration string,
           date string,
           strike float,
           last float,
           bid float,
           ask float,
           volume float,
           open_interest int32,
           t1_open_interest int32,
           iv_mean float,
           iv_bid float,
           iv_ask float,
           delta float,
           gamma float,
           theta float,
           vega float,
           aka string
        )
   );

/* create a binding `binding01` 
   with public (!) access for DataLens connection
   use required data types
   with use of bindings */

SELECT
    COUNT(*)
FROM
    bindings.`binding01`

/* groupby `base_symbol` with bindings */

SELECT
    base_symbol,
    COUNT(*) as counts
FROM
    bindings.`binding01`
GROUP BY base_symbol

/* groupby `type` with bindings */

SELECT
   type,
   COUNT(*) as counts
FROM
   bindings.`binding01`
GROUP BY type;

/* or with sum */

SELECT
   type,
   SUM(volume) as total
FROM
   bindings.`binding01`
GROUP BY type

/* create another binding `binding02` 
   with public (!) access for DataLens connection
   with fields that are neede for groupby
   use required data type
   with use of bindings */

INSERT INTO bindings.`binding02` ( option_symbol, counts)
SELECT
   option_symbol as option_symbol,
   COUNT(*) as counts
FROM
   bindings.`binding01`
GROUP BY option_symbol

/* Marlet Cap data */

SELECT
   *
FROM
   bindings.`binding03`
LIMIT 10;

/* or */

SELECT
   Industry,
   SUM(`Market Cap`) as total_cap
FROM
   bindings.`binding03`
GROUP BY Industry
ORDER BY total_cap DESC
