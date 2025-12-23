select * from Amazon 


-- DATA CLEANING STEP:
-- This query is used during the data cleaning process
-- to identify and measure missing (NULL) values


select 
sum(case when OrderID is null then 1 else 0 end),
sum(case when OrderDate is null then 1 else 0 end) , 
sum(case when CustomerID is null then 1 else 0 end) , 
sum(case when CustomerName is null then 1 else 0 end),
sum(case when ProductID is null then 1 else 0 end) , 
sum(case when Category is null then 1 else 0 end) , 
sum(case when Quantity is null then 1 else 0 end), 
sum(case when UnitPrice is null then 1 else 0 end)
from Amazon


-- DATA CLEANING STEP:
-- This query is part of the data cleaning process
-- and is used to detect duplicate records.


SELECT
    orderid,
    customerid,
    productid,
    orderdate,
    quantity,
    UnitPrice,
    COUNT(*) AS cnt
FROM Amazon
GROUP BY
     orderid,
    customerid,
    productid,
    orderdate,
    quantity,
    UnitPrice
HAVING COUNT(*) > 1;
/*
This query aggregates Amazon sales data on a monthly level and calculates
key sales performance KPIs.

It computes:
- Monthly sales quantity (QTY_Sales)
- Number of orders per month (NUM_of_orders)
- Total monthly sales revenue (Total_Sales)
- Average Order Value (AVR_Order)
- Year-to-Date (YTD) total sales using a window function
- Month-over-Month (MoM) sales change
- Year-over-Year (YoY) sales change 
*/ 

with table1 as (
select 
DATETRUNC (MONTH , OrderDate) as Date_ , 
SUM(Quantity) as QTY_Sales , 
COUNT(DISTINCT OrderID) as NUM_oF_orders , 
round (sum (quantity*unitprice),2) as total_sales 
from 
Amazon
Group by DATETRUNC (MONTH , OrderDate))
select 
date_ , 
QTY_sales , 
NUM_of_orders ,
round (total_sales / NUM_of_orders ,2) as AVR_order , 
Total_sales , 
SUM (total_sales) over (partition by year(date_) order by (date_) )as TOTAL_YTD , 
lag (total_sales,1) over (order by (date_) ) as pre_month  , 
round (total_sales - lag (total_sales,1) over (order by (date_)),2) as MOM  , 
lag (total_sales , 12) over (order by (date_)) as pre_year  , 
round (total_sales - lag (total_sales , 12) over (order by (date_)),2) as YOY 
from table1  


/* 
This query analyzes product-level sales performance by category.

It calculates:
- Total quantity sold per product (QTY_Sales)
- Number of distinct orders per product (NUM_of_orders)
- Total sales revenue per product (Total_Sales)

The data is grouped by Category and ProductName to evaluate
product performance and identify top-selling products.
*/
select 
Category ,
ProductName , 
SUM(Quantity) as QTY_Sales , 
COUNT(DISTINCT OrderID) as NUM_oF_orders , 
round (sum (quantity*unitprice),2) as total_sales 
from 
Amazon 
group by Category , ProductName