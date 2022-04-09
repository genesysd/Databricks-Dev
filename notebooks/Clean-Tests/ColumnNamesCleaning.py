# Databricks notebook source
spark.sql("SHOW databases").show(truncate=True)


# COMMAND ----------

spark.sql("USE salesforce_sandbox")
spark.sql("SHOW tables").show()

# COMMAND ----------

df = sqlContext.table("salesforce_sandbox.account")
df.printSchema()

# COMMAND ----------

# Column Names Cleaning

def clean_cols(listOfTuples):
    """
    Input  => List of Tuples. Ex df.dtypes = [('id', 'string'), ('is_deleted', 'boolean')]
    Output => Clean Col Names List 
    Clean Logic => Removed _c for all datatypes
         For timestamp => added _utc_ts
         For boolean   => added _flag
    """
    schema_new=[]
    for i in listOfTuples:
        if i[1].lower() == 'boolean':
            if i[0].lower().endswith("_c") == True:
                s = i[0][:-2]+'_flag'
                schema_new.append(s)
            else:
                schema_new.append(i[0]+'_flag')
        elif i[1].lower() == 'timestamp':
            if i[0].lower().endswith("_c") == True:
                s = i[0][:-2]+'_utc_ts'
                schema_new.append(s)
            else:
                schema_new.append(i[0]+'_utc_ts')
        else:
            if i[0].lower().endswith("_c") == True:
                schema_new.append(i[0][:-2])
            else:
                schema_new.append(i[0])
    return schema_new
                

# # print(len(schema_new))        
# # print(len(df.columns))
df2 = df.toDF(*clean_cols(df.dtypes))
# display(df2)
print(df.count())
print(df2.count())
df2.printSchema()
