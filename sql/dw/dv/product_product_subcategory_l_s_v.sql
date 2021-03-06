--   ======================================================================================
--    AUTOGENERATED!!!! DO NOT EDIT!!!!
--   ======================================================================================

CREATE OR REPLACE VIEW dv.product_product_subcategory_l_s
AS
SELECT
  product_product_subcategory_l_key
  ,max(load_dts) AS load_dts
  ,effective_ts
  ,rec_src
FROM (
  
  SELECT
    src:ProductID::STRING ||'|'|| src:ProductSubcategoryID::STRING AS product_product_subcategory_l_key
    ,CURRENT_TIMESTAMP AS load_dts
    ,src:ModifiedDate::TIMESTAMP AS effective_ts
    ,'sales' AS rec_src
  FROM
    src_adventureworks.product
)
GROUP BY
  product_product_subcategory_l_key
  ,effective_ts
  ,rec_src
;