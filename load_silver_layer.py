
class LoadSilverLayer:
    def __init__(self, spark, table_name_to_use_sql, output_path):
        self.spark = spark
        self.table_name = table_name_to_use_sql
        self.output_path = output_path
    
    def transform(self, sql, raw_data):
        #table = self.spark.read.parquet(self.inputPath)
        raw_data.createOrReplaceTempView(self.table_name)
        self.final_table = self.spark.sql(sql)
        
        return self
    
    def write_silver_layer(self, isPartitionBy, partitionColumns):
        if isPartitionBy:
            self.final_table.write.mode("overwrite").partitionBy(partitionColumns).parquet(self.output_path)
        else:
            self.final_table.write.mode("overwrite").parquet(self.output_path)
        return self.final_table