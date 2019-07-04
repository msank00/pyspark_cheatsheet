# PySpark CheatSheet

Let's talk about this... `inport numpy as np`

- bullet 1
  - bullet 1,1
- bullet 2

```py
sdf = Spark Data Frame 
```

## Get column names
```py
ls_col = sdf.columns
```

## Get dtypes
```py
ls_dtype = sdf.dtypes
```

## Select columns from sdf

```py
col_list = ['claim_no','policy_no', 'product_type' ]  
sdf = sdf.select(col_list)
```
## Select columns + where from sdf

```py
col_list = ['claim_no','policy_no', 'product_type' ]  
sdf = sdf.select(col_list)
sdf = sdf.filter(sdf.claim_no.isin('values'))
```
[AnalyticsVidya](https://www.analyticsvidhya.com/blog/2016/10/spark-dataframe-and-operations/)

## Get NULL count for sdf

### Get NULL Count

```py
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, when, isnan

n_row = sdf.count()
print("total rows: {}".format(n_row))
sdf_null_count = sdf.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in sdf.columns])
```

### Get NULL PCT(%) Count
```py
sdf = sdf_null_count 
print("Percentage Count started....")
for field in sdf.schema.fields:
    if str(field.dataType) in ['DoubleType', 'FloatType', 'LongType', 'IntegerType', 'DecimalType']:
        name = str(field.name)
        sdf = sdf.withColumn(name, col(name)/CONSTANT)
```

## Drop list of Column if all entries in a spark dataframe's specific column is null

```py
# Function to drop the empty columns of a DF
def dropNullColumns(df):
    # A set of all the null values you can encounter
    null_set = {"none", "null" , "nan"}
    # Iterate over each column in the DF
    for col in df.columns:
        # Get the distinct values of the column
        unique_val = df.select(col).distinct().collect()[0][0]
        # See whether the unique value is only none/nan or null
        if str(unique_val).lower() in null_set:
            print("Dropping " + col + " because of all null values.")
            df = df.drop(col)
    return(df)
```

- [stackoverflow](https://stackoverflow.com/questions/45629781/drop-if-all-entries-in-a-spark-dataframes-specific-column-is-null)


## Get Groupby Count of categorical columns

```py
sdf_cat_level_count = sdf.groupby('CATEGORICAL_COLUMN_NAME').count() 
```

## Difference between 2 timestamp
```py
from pyspark.sql import functions as F
timeFmt = "yyyy-MM-dd'T'HH:mm:ss.SSS"
timeDiff = (F.unix_timestamp('End_date_<col_name>', format=timeFmt)
            - F.unix_timestamp('Start_date_<col_name>', format=timeFmt))
df = df.withColumn("Duration", timeDiff)
```



[SO_link](https://stackoverflow.com/questions/44821206/pyspark-difference-between-two-dates-cast-timestamptype-datediff?rq=1)

##  Number of unique values in a column

```py
col_nmae = "PRODUCT_TYPE"
sdf.select(col_name).distinct().count()
```

## Fill NA with median for multiple columns 

```py
# Three parameters have to be passed through approxQuantile function
# 1. col – the name of the numerical column
# 2. probabilities – a list of quantile probabilities Each number must belong to [0, 1]. For example 0 is the minimum, 0.5 is the median, 1 is the maximum.
# 3. relativeError – The relative target precision to achieve (>= 0). If set to zero, the exact quantiles are computed, which could be very expensive. Note that values greater than 1 are accepted but give the same result as 1.
median=sdf.approxQuantile('Total Volume',[0.5],0.1)
print ('The median of Total Volume is '+str(median))
```

[Ref: Blog](https://blog.usejournal.com/data-wrangling-in-pyspark-4cb3c88ca4e0)

## CrossValidation model

```py
    # paramGrid = (ParamGridBuilder()
    #             .addGrid(rf.maxDepth, [5, 8])
    #             .addGrid(rf.maxBins, [60]) # *******************check this parameter**********************
    #             .addGrid(rf.numTrees, [500])
    #             .build())

    # # .addGrid(rf.minInstancesPerNode, [1, 2, 3])

    # cv = CrossValidator(estimator=rf, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=3)
    # # Run cross validations.  This can take about 6 minutes since it is training over 20 trees!
    # log.info("... Cross validated training started...")
    # start_time = time.time()
    # cvModel = cv.fit(trainingData)
    # end_time = time.time()
    # log.info("... Cross validated Regression Model Training Execution Time: {} seconds".format(end_time-start_time))

    # # Use test set here so we can measure the accuracy of our model on new data
    # cvmodel_predictions = cvModel.transform(testData)
    # log.info("... With CV, Root Mean Squared Error (RMSE) on test data = {}".format(evaluator.evaluate(cvmodel_predictions)))

    # bestModel = cvModel.bestModel
    # finalPredictions = bestModel.transform(spark_df)
    # log.info("... With CV, Best Model Root Mean Squared Error (RMSE) on full data = {}".format(evaluator.evaluate(finalPredictions)))
```

## histogram plot

```py
# Doing the heavy lifting in Spark. We could leverage the `histogram` function from the RDD api

gre_histogram = df_spark.select('col_name').rdd.flatMap(lambda x: x).histogram(11)

# Loading the Computed Histogram into a Pandas Dataframe for plotting
pd.DataFrame(
    list(zip(*gre_histogram)), 
    columns=['bin', 'frequency']
).set_index(
    'bin'
).plot(kind='bar')
```

[SO](https://stackoverflow.com/questions/39154325/pyspark-show-histogram-of-a-data-frame-column)


## Median finding - Multiple column

**Note:** Remember that `sdf.stat.approxQuantile()` always returs a `list`

```py
def roughfix_spark(sdf):

    # URL: https://stackoverflow.com/questions/45287832/pyspark-approxquantile-function

    col_float = [item[0] for item in sdf.dtypes if item[1].startswith('float')] 
    col_decimal = [item[0] for item in sdf.dtypes if item[1].startswith('decimal')] 
    col_numeric = sorted(col_float + col_decimal)

    median_dict = {}
    ls_missing_median = []
    for c in col_numeric:
        median = sdf.stat.approxQuantile(c, [0.5], 0.1)
        if len(median) == 1:
            median_dict[c] = median[0]
        elif len(median) == 0:
            ls_missing_median.append(c)

    return sdf.na.fill(median_dict)
```

## Reference:

- [AnalyticsVidya](https://www.analyticsvidhya.com/blog/2016/10/spark-dataframe-and-operations/)
- [PySpark_SQL_Cheat_Sheet](https://s3.amazonaws.com/assets.datacamp.com/blog_assets/PySpark_SQL_Cheat_Sheet_Python.pdf)
- [pyspark-cheatsheet](https://www.qubole.com/resources/pyspark-cheatsheet/)
- [pyspark-dataframe-tutorial](https://www.edureka.co/blog/pyspark-dataframe-tutorial/)
