--   ======================================================================================
--    AUTOGENERATED!!!! DO NOT EDIT!!!!
--   ======================================================================================

CREATE OR REPLACE VIEW dv.sales_mesaures_s
AS
SELECT
  sales_key
  ,max(load_dts) AS load_dts
  ,effective_ts
  ,freight
  ,subtotal
  ,taxamt
  ,totaldue
  ,rec_src
FROM (
  
  SELECT
    src:SalesOrderID::STRING AS sales_key
    ,CURRENT_TIMESTAMP AS load_dts
    ,src:ModifiedDate::TIMESTAMP AS effective_ts
    ,src:Freight::NUMBER(18,4) AS freight
    ,src:SubTotal::NUMBER(18,4) AS subtotal
    ,src:TaxAmt::NUMBER(18,4) AS taxamt
    ,src:TotalDue::NUMBER(18,4) AS totaldue
    ,'sales' AS rec_src
  FROM
    src_adventureworks.salesorderheader
)
GROUP BY
  sales_key
  ,effective_ts
  ,freight
  ,subtotal
  ,taxamt
  ,totaldue
  ,rec_src
;