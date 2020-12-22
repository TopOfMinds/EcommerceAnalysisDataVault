CREATE OR REPLACE VIEW dv.product_main_sb
AS
SELECT
    row_number() OVER(ORDER BY effective_ts, product_key) AS product_main_s_key
    ,* 
FROM product_main_s;
;
-- the row number should be based on load_dts, product_key to make it stable
-- but in this case load_dts is calculated dynamically in a view, so it wouldn't work