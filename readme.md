# README

## Description
This code is written in a Jupyter Notebook and is primarily focused on processing and manipulating datasets using PySpark, which is a Python library for big data processing. The code reads two CSV datasets, performs filtering, joins the datasets, renames columns, and selects specific columns. Additionally, it logs information about the execution and writes the log data to a CSV file.

## Dependencies
To run this code, you need to have the following dependencies installed:
- PySpark

You can install these dependencies using the following commands:

```python
!pip install pyspark
```

## Usage
1. Import the necessary libraries and dependencies.
2. Set up the SparkSession using `SparkSession.builder`.
3. Define the `log()` function to log information about the execution.
4. Set the paths for `ds1_path` and `ds2_path` variables to the location of your dataset files.
5. Set the `country_names` variable to a list of countries you want to filter on.
6. Read the datasets (`ds1` and `ds2`) using `spark.read.csv()` function.
7. Filter `ds1` based on the specified country names using the `to_filter()` function.
8. Join the filtered dataset `ds1` with `ds2` on the 'id' column using `join()` function.
9. Rename the desired columns using the `rename()` function.
10. Select the required columns from the joined dataset.
11. Display the final dataset using `show()`.

Note: Make sure to update the paths, country names, and any other necessary parameters according to your specific requirements.

## Logging
The code logs information about the execution using the `log()` function. The logged data includes the log date, status (Success/Failed), log text, and row count. The log data is written to a CSV file named 'logfile.csv' in the '/content/' directory. You can uncomment the relevant lines of code to enable logging and display the logged data using `spark.read.csv()` and `show()`.

Please ensure that you have write permissions to the '/content/' directory to store the log file.

## Additional Notes
- The code assumes that the datasets are in CSV format with headers.
- The code handles exceptions using `try-except` blocks and logs any exceptions that occur during execution.
- The code uses various PySpark functions such as `filter()`, `join()`, `withColumnRenamed()`, `select()`, and `show()` to process the datasets.
- The code uses Python's `datetime` module to get the current date and time.

