CREATE OR REPLACE VIEW dv.sales_context_sb
AS
SELECT
    row_number() OVER(ORDER BY effective_ts, sales_key) AS sales_s_key
    ,* 
FROM dv.sales_context_s
;
-- the row number should be based on load_dts, sales_key to make it stable
-- but in this case load_dts is calculated dynamically in a view, so it wouldn't work