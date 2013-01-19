--average_dividend.pig
-- load data from NYSE_dividends, declaring the schema to have 4 fields
sum_data = load 'NYSE_dividends' as (exchange, symbol, date, dividend);
-- calculate the sum dividend 
sum_result = SUM(sum_data.dividend);
-- store the results to average_dividend
store sum_result into 'sum_dividend_test';
