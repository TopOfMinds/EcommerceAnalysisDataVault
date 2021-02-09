CREATE OR REPLACE VIEW dv.product_subcategory_sb
AS
SELECT
    row_number() OVER(ORDER BY effective_ts, product_subcategory_key) AS product_subcategory_s_key
    ,* 
FROM product_subcategory_s;
;
-- the row number should be based on load_dts, product_subcategory_key to make it stable
-- but in this case load_dts is calculated dynamically in a view, so it wouldn't work