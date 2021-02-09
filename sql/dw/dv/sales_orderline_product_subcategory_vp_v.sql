CREATE OR REPLACE VIEW dv.sales_orderline_product_subcategory_vp
AS
WITH p_s_sb AS (
  SELECT
      product_subcategory_s_key
      , product_subcategory_key
      , effective_ts
      , coalesce(lead(effective_ts) OVER(PARTITION BY product_subcategory_key ORDER BY effective_ts), to_date('9999-12-31', 'yyyy-mm-dd')) AS effective_ts_end 
  FROM dv.product_subcategory_sb
)
, p_p_s_l_s AS (
  SELECT
    ppsls.product_product_subcategory_l_key
    , ppsls.effective_ts
    , coalesce(lead(ppsls.effective_ts) OVER(PARTITION BY ppsl.product_key ORDER BY ppsls.effective_ts), to_date('9999-12-31', 'yyyy-mm-dd')) AS effective_ts_end
  FROM dv.product_product_subcategory_l_s ppsls
  JOIN dv.product_product_subcategory_l ppsl ON ppsls.product_product_subcategory_l_key = ppsl.product_product_subcategory_l_key
)
SELECT
    so.sales_orderline_key
    , pssb.product_subcategory_s_key
    , CURRENT_TIMESTAMP AS load_dts
FROM dv.sales_orderline_measures_s so
JOIN dv.sales_orderline_product_l sop ON sop.sales_orderline_key = so.sales_orderline_key
JOIN dv.product_product_subcategory_l ppsl ON ppsl.product_key = sop.product_key
JOIN p_p_s_l_s ppsls ON ppsls.product_product_subcategory_l_key = ppsl.product_product_subcategory_l_key -- AND ppsls.effective_ts <= so.effective_ts AND so.effective_ts < ppsls.effective_ts_end
JOIN p_s_sb pssb ON pssb.product_subcategory_key = ppsl.product_subcategory_key AND pssb.effective_ts <= so.effective_ts AND so.effective_ts < pssb.effective_ts_end
;
-- the product table only contains 1 row per product and the effective_ts is after the effective_ts of the sales order
