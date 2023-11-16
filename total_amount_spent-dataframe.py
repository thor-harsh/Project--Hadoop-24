from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType,StructField,FloatType,IntegerType,StringType


spark=SparkSession.builder.appName('total-amount-spent').getOrCreate()

schema=StructType([
    StructField('Customer_Id',StringType(),True),
    StructField('Product_Id',StringType(),True),
    StructField('Amount',FloatType(),True)
])

#Read the file as a dataframe
df=spark.read.schema(schema).csv('customer-orders.csv')
df.printSchema()

#Grouping customers with their customer id and total amount spent
amount=df.groupby('Customer_Id').agg(func.round(func.sum('Amount'),2).alias('total_spent'))

final_sort=amount.sort('total_spent')
final_sort.show(final_sort.count())

spark.stop()





