CREATE OR REPLACE VIEW dv.sales_orderline_sales_context_vp
AS
WITH s_c_s AS (
  SELECT
      sales_s_key
      , sales_key
      , effective_ts
      , coalesce(lead(effective_ts) OVER(PARTITION BY sales_key ORDER BY effective_ts), to_date('9999-12-31', 'yyyy-mm-dd')) AS effective_ts_end 
  FROM dv.sales_context_sb
)
SELECT
    so.sales_orderline_key
    , scs.sales_s_key
    , CURRENT_TIMESTAMP AS load_dts
FROM dv.sales_orderline_measures_s so
JOIN dv.sales_sales_orderline_l ssol ON ssol.sales_orderline_key = so.sales_orderline_key
JOIN s_c_s scs ON scs.sales_key = ssol.sales_key -- AND scs.effective_ts <= so.effective_ts AND so.effective_ts < scs.effective_ts_end
;
-- the sales table only contains 1 row per product and the effective_ts is after the effective_ts of the sales order
