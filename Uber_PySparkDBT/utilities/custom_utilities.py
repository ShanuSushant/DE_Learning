from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

class transformations:

    def dedup(self,df:DataFrame,dedup_columns:List,cdc:str):

        df = df.withColumn("dedupkey",concat(*dedup_columns))
        df = df.withColumn("dedupCounts",row_number().over(Window.partitionBy("dedupkey").orderBy(desc(cdc))))
        df = df.filter(col("dedupCounts")==1)
        df = df.drop("dedupkey","dedupCounts")
        return df
    
    def processing_timestamp(self,df:DataFrame):

        df = df.withColumn("processing_timestamp",current_timestamp())
        return df
    
    def upsert(self,df,key_cols,table,cdc):

        merge_condition = " AND ".join([f"src.{i} = trg.{i}" for i in key_cols])
        dlt_obj = DeltaTable.forName(spark,f"pysparkdbt.silver.{table}")
        dlt_obj.alias("trg").merge(df.alias("src"),merge_condition)\
                            .whenMatchedUpdateAll()\
                            .whenNotMatchedInsertAll()\
                            .execute()
        return "Upsert Completed!"