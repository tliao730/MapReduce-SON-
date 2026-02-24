SON Algorithm for Frequent Itemset Mining (PySpark RDD)

## 1. Project OverviewThis project implements the SON Algorithm using the Spark RDD framework to identify frequent itemsets in large-scale datasets. The goal is to apply distributed computing techniques to find all possible combinations of frequent itemsets efficiently within required time constraints.

## 2. Problem StatementFrequent itemset mining on massive datasets is computationally expensive. This project addresses this by implementing a limited-pass algorithm (SON Algorithm) that divides the data into chunks, finds local candidates using an A-Priori approach, and then validates these candidates globally in a second pass.

## 3. Requirements and EnvironmentProgramming Language: Python 3.6.+1 Spark Version: Spark 3.1.2.Java Version: JDK 1.8.Library Constraints: Only standard Python libraries are used. Spark RDD operations are used exclusively; Spark DataFrame and DataSet are strictly prohibited.

## 4. Dataset DescriptionTask 1: Simulated Data Input Files: small1.csv (for debugging) and small2.csv (for testing).Case 1: Find frequent business IDs reviewed by users (User-Business model).+1Case 2: Find frequent user IDs who commented on businesses (Business-User model). Task 2: Ta Feng Data Preprocessing: Aggregates all purchases made by a customer within a single day into one basket.Customer ID Format: Renamed as DATE-CUSTOMER_ID (e.g., 11/14/00-12321).Filter Threshold: A threshold k is applied to filter out customers who purchased fewer than k items.

## 5. Execution Instructions
Tasks are executed via spark-submit. Ensure the Python version is set to 3.6.

Task 1 Execution

```Bash
spark-submit task1.py <case_number> <support> <input_file_path> <output_file_path>
```

case_number: 1 for Case 1, 2 for Case 2.


support: The minimum count to qualify as a frequent itemset.

Task 2 Execution
```Bash
spark-submit task2.py <filter_threshold> <support> <input_file_path> <output_file_path>
```

filter_threshold: Used to filter out qualified users.

## 6. Output Format
The program generates a single output file and prints the execution duration to the console.

Console Output
- Must include the total execution time with the "Duration" tag (e.g., Duration: 100).

Output File Structure

1. Candidates: Local frequent itemsets found after the first pass of the SON Algorithm.
2. Frequent Itemsets: Final frequent itemsets verified after the second pass.
3. Formatting Rules:

- Itemsets are grouped by size (singletons, pairs, triples, etc.).

- Groups are separated by an empty line.

- All itemsets are sorted in lexicographical order.

- Example singleton format: ('100').

- Example pair format: ('100', '101').

## 7. Performance Benchmarks
The implementation is optimized to meet the following execution time requirements on Vocareum:
- Task 1 (Case 1, Support 4): ≤ 200 seconds.
- Task 1 (Case 2, Support 9): ≤ 100 seconds.
- Task 2 (Filter 20, Support 50): ≤ 500 seconds.