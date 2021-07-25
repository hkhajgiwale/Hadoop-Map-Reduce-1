# HadoopGeoSalesMapReduce
Hadoop Big Data Analysis for Sales from Different Countries

### Problem Statement: 
A large multi-national retail chain has sales orders data across regions and different sales channels
for a large variety of item types. The business team wants to use this data to analyze various aspects
of sales - e.g. top selling items in a region, regions with maximum profit in a certain item type, if there
is a significant difference in revenue in two item types across regions etc.

### CSV File
CSV file is located in resources directory: [GeoSalesCSVFile](https://raw.githubusercontent.com/hkhajgiwale/HadoopGeoSalesMapReduce/master/src/main/resources/geosales_dataset.csv)


## Question 1

#### Question:
Find Average unit_price by country for a given item type in a certain year
#### Solution:
1. In code [SalesMapReduce1](https://github.com/hkhajgiwale/HadoopGeoSalesMapReduce/blob/master/src/main/java/org/harsh/hadoop/bigdata/SalesMapReduce1.java), based on hashcode sorting is done
2. Sorting is done on the basis on country, item_type and year
3. Custom paritioner class is created
4. Sales class created and its object is created that comprises of country, item_type and year
5. Object of sales class is created and it is passed as key to mapper and value is the unit price whose average is to be calculated
6. In reducer code, over the values of unit price is iterated and average is calculated


## Question 2
#### Question:
Total units_sold by year for a given country and a given item type
#### Solution:
1. In code [SalesMapReduce2](https://github.com/hkhajgiwale/HadoopGeoSalesMapReduce/blob/master/src/main/java/org/harsh/hadoop/bigdata/SalesMapReduce2.java), based on hashcode sorting is done
2. Sorting is done on the basis on country, item_type and year
3. Custom paritioner class is created
4. Sales class created and its object is created that comprises of country, item_type and year
5. Object of sales class is created and it is passed as key to mapper and value is the units sold whose total is to be calculated
6. In reducer code, over the values of unit price is iterated and total number of units_solds is calculated


## Question 3
#### Question:
Find the max and min units_sold in any order for each year by country for a given item type. Use a
custom partitioner class instead of default hash based.
#### Solution:
1. In code [SalesMapReduce3](https://github.com/hkhajgiwale/HadoopGeoSalesMapReduce/blob/master/src/main/java/org/harsh/hadoop/bigdata/SalesMapReduce3.java), based on hashcode sorting is done
2. Sorting is done on the basis on country, item_type and year
3. Custom paritioner class is created
4. Sales class created and its object is created that comprises of country, item_type and year
5.Object of sales class is created and its passed as the key to mapper and value is the units_sold
6. In reducer code, over the value of units_sold is iterated, minimum and maximum of units sold for a given is calculated from there.

## Question 4
#### Question:
What are the top 10 order id for a given year by the total_profit
#### Solution:
1. In code [SalesMapReduce4](https://github.com/hkhajgiwale/HadoopGeoSalesMapReduce/blob/master/src/main/java/org/harsh/hadoop/bigdata/SalesMapReduce4.java), based on hashcode sorting is done
2. Sorting is done on the basis of total_profit
3. In mapper, Adding entries to hashmap and then creating TreeSet object to get the ordered profits as per year and since a set, it is unique
4. Since we have to remove the top 10 records, when added one extra after 10, it is removed. Adding unique ordered profits to the treemap
5. Fetching entries from HashMap and adding to the context. This output of mapper would be served as input to reducer
5. In reducer, HashMap where year is the key and sales object that has orderedID and total profit is provided
6. Creating tree set to put total profits and orderid that are ordered per year
7. Iterating over sales object to get top 10 records, remove the one that is added after 10 and terminate the loop
8. Adding these to the context of reducer would yeild top 10 order id per year based on total profit


