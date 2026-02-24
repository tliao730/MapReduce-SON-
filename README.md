SON Algorithm for Frequent Itemset Mining (PySpark RDD)

1. Project OverviewThis project implements the SON Algorithm using the Spark RDD framework to identify frequent itemsets in large-scale datasets. The goal is to apply distributed computing techniques to find all possible combinations of frequent itemsets efficiently within required time constraints.

2. Problem StatementFrequent itemset mining on massive datasets is computationally expensive. This project addresses this by implementing a limited-pass algorithm (SON Algorithm) that divides the data into chunks, finds local candidates using an A-Priori approach, and then validates these candidates globally in a second pass.

3. Requirements and EnvironmentProgramming Language: Python 3.6.+1Spark Version: Spark 3.1.2.Java Version: JDK 1.8.Library Constraints: Only standard Python libraries are used. Spark RDD operations are used exclusively; Spark DataFrame and DataSet are strictly prohibited.+2

4. Dataset DescriptionTask 1: Simulated Data Input Files: small1.csv (for debugging) and small2.csv (for testing).Case 1: Find frequent business IDs reviewed by users (User-Business model).+1Case 2: Find frequent user IDs who commented on businesses (Business-User model).+1Task 2: Ta Feng Data Preprocessing: Aggregates all purchases made by a customer within a single day into one basket.Customer ID Format: Renamed as DATE-CUSTOMER_ID (e.g., 11/14/00-12321).Filter Threshold: A threshold $k$ is applied to filter out customers who purchased fewer than $k$ items.+1