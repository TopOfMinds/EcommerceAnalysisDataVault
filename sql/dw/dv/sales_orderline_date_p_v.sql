CREATE OR REPLACE VIEW dv.sales_orderline_date_p
AS
SELECT
    soms.sales_orderline_key
    , to_date(soms.effective_ts) AS date_key
    , CURRENT_TIMESTAMP AS load_dts
FROM dv.sales_orderline_measures_s soms;
