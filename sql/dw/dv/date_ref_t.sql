CREATE OR REPLACE table dv.date_ref (
  date_key         date        NOT NULL
 ,year_quarter     string      NOT NULL
 ,year_month       string        NOT NULL
 ,year_week        string        NOT NULL
 ,year             smallint    NOT NULL
 ,month            smallint    NOT NULL
 ,month_name       string        NOT NULL
 ,day_of_month     smallint    NOT NULL
 ,day_of_week      string      NOT NULL
 ,week_of_year     smallint    NOT NULL
 ,day_of_year      smallint    NOT NULL
)
AS
 WITH cte_my_date AS (
   SELECT DATEADD(DAY, SEQ4(), '2000-01-01') AS my_date
     FROM TABLE(GENERATOR(ROWCOUNT=>20000))  -- Number of days after reference date in previous line
 )
 SELECT my_date
       ,YEAR(my_date) || '-Q' || QUARTER(my_date)
       ,YEAR(my_date) || '-' || LPAD(MONTH(my_date), 2, '0')
       ,YEAR(my_date) || '-V' || LPAD(WEEKOFYEAR(my_date), 2, '0')
       ,YEAR(my_date)
       ,MONTH(my_date)
       ,MONTHNAME(my_date)
       ,DAY(my_date)
       ,DAYOFWEEKISO(my_date)
       ,WEEKOFYEAR(my_date)
       ,DAYOFYEAR(my_date)
   FROM cte_my_date
;