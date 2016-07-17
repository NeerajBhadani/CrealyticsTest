# Problem Statement : Aggregate the data by date including columns for each type given in the input.

# Import the Required Libraries.
import pandas as pd
import numpy as np
from pyspark import SparkContext, SparkConf


# Creating a Object of Spark Context which is starting point of Spark Program.
conf = SparkConf().setAppName("challenge")
sctxt = SparkContext(conf=conf)

# Load the data file into RDD:input.
raw_file = sctxt.textFile('data/crealytics')

# Remove Header. RDD:data contains data without header
lines = raw_file.map(lambda line : line.split(";"))
header = lines.first()
data = lines.filter(lambda line : line[0] != "date")

# Get the Columns for Result.
cols =  data.map(lambda x : x[1]).distinct().collect()

# Add date column to be added to result.
cols.insert(0,"date")

# Get all the Dates from Data.
dt = data.map(lambda x: x[0]).distinct().collect()
oldest_dt = min(dt)
newest_dt = max(dt)

# Create List of dates between the oldest and newest date in DatSet.
dateList = pd.date_range(oldest_dt, newest_dt)


# Create Empty DF.
# Get the No. of columns and Rows
no_cols = len(cols)
no_rows = len(dateList)

# Create Empty DataFrame.
stagedf = pd.DataFrame(np.zeros((no_rows,no_cols)), columns=cols)

# Add the dates to result DF.
stagedf['date'] = dateList

# Aggregate by date including columns for each type
dt_type = data.map(lambda x :  ((x[0],x[1]), int(x[2])))
count = dt_type.reduceByKey(lambda x,y : x + y).collect()

# Create new dataframe which is sorted By date and set the date as Index.
resultdf = stagedf.sort_values(by='date').set_index('date')

# Add the aggregated values to resultdf.
# dt : contains Date
# tp : Contains Type.
# val : Contains Aggregated Value.
for i in range(len(count)):
    dt = count[i][0][0]
    tp = count[i][0][1]
    val = count[i][1]
    
    resultdf.set_value(dt,tp,val)

# save the result in file "result.csv" with ";" seperator
resultdf.to_csv("result.csv", sep=';')