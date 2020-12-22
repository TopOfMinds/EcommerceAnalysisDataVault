CREATE OR REPLACE VIEW dv.sales_orderline_product_hl
AS
WITH product AS (
  SELECT
      product_main_s_key
      , product_key
      , effective_ts
      , coalesce(lead(effective_ts) OVER(PARTITION BY product_key ORDER BY effective_ts), to_date('9999-12-31', 'yyyy-mm-dd')) AS effective_ts_end 
  FROM product_main_sb
)
SELECT
    so.sales_orderline_key
    ,p.product_main_s_key
FROM sales_orderline_measures_s so
JOIN sales_orderline_product_l sop ON sop.sales_orderline_key = so.sales_orderline_key
JOIN product p ON p.product_key = sop.product_key -- and p.effective_ts <= so.effective_ts and so.effective_ts < p.effective_ts_end
;
-- the product table only contains 1 row per product and the effective_ts is after the effective_ts of the sales order
