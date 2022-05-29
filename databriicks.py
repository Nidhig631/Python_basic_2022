# Databricks notebook source
#/FileStore/tables/order.csv
# sparksession object spark
#df=spark.read.csv('/FileStore/tables/ORD.csv',header=True,inferSchema=True,sep=',')
#df.show()
#---print schema datatypes and column
#df.printSchema()  
#display(df)


# COMMAND ----------

from pyspark.sql.types import *
orderSchema = StructType([StructField("ID",StringType(),True),              StructField("NAME",StringType(),True),
                  StructField("Place",StringType(),True)
])
df = spark.read.load("/FileStore/tables/ORD.csv",format="csv",header=True,schema=orderSchema)
df.printSchema()
df.schema
                   

# COMMAND ----------

OrderSchema = 'ID String, Name String, Place String'
df = spark.read.load("/FileStore/tables/ORD.csv",format="csv",header=True,schema=OrderSchema)
df.printSchema()



# COMMAND ----------

#dataframe is set of rows and columns

from pyspark.sql.functions import expr, col, column
df.select("ID","Name",col("Place"),column("Place"),
         expr("ID = '1'")).show(2)


# COMMAND ----------

# add new column
from pyspark.sql.functions import lit
extractdf1 = df.withColumn("dayOfPurchase",lit("Sunday"))
extractdf2 = df.withColumn("place",expr("place<>'India'"))
#display(extractdf1)
display(extractdf2)

# COMMAND ----------

renamedf = df.withColumnRenamed("place","country")
display(renamedf)

# COMMAND ----------

renamedf= df.withColumnRenamed("place","renameCountry")
display(renamedf)
renamedf.select("renameCountry").show()

# COMMAND ----------

newdf =df.drop("place")
display(newdf)

# COMMAND ----------

newdf = df.withColumn("NewDataType", col("id").cast("double"))
newdf.printSchema()

# COMMAND ----------

newdf = df.filter(col("id")>2)
display(newdf)

# COMMAND ----------

#sort or orderBy
df = df.sort(expr("ID desc")
display(df)

# COMMAND ----------


