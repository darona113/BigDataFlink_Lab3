-- Проверочные запросы после загрузки данных
SELECT COUNT(*) AS customer_count FROM dim_customer;
SELECT COUNT(*) AS seller_count FROM dim_seller;
SELECT COUNT(*) AS store_count FROM dim_store;
SELECT COUNT(*) AS supplier_count FROM dim_supplier;
SELECT COUNT(*) AS product_count FROM dim_product;
SELECT COUNT(*) AS date_count FROM dim_date;
SELECT COUNT(*) AS fact_count FROM fact_sales;

SELECT source_file, COUNT(*) AS rows_loaded
FROM fact_sales
GROUP BY source_file
ORDER BY source_file;
