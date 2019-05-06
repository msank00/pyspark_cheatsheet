# PySpark CheatSheet

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

**Get NULL Count**

```py
n_row = sdf.count()
print("total rows: {}".format(n_row))
sdf_null_count = sdf.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in sdf.columns])
```
**Get NULL PCT(%) Count**
```py
sdf = sdf_null_count 
print("Percentage Count started....")
for field in sdf.schema.fields:
    if str(field.dataType) in ['DoubleType', 'FloatType', 'LongType', 'IntegerType', 'DecimalType']:
        name = str(field.name)
        sdf = sdf.withColumn(name, col(name)/CONSTANT)
```
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

## Reference:

- [AnalyticsVidya](https://www.analyticsvidhya.com/blog/2016/10/spark-dataframe-and-operations/)
- [PySpark_SQL_Cheat_Sheet](https://s3.amazonaws.com/assets.datacamp.com/blog_assets/PySpark_SQL_Cheat_Sheet_Python.pdf)
- [pyspark-cheatsheet](https://www.qubole.com/resources/pyspark-cheatsheet/)
